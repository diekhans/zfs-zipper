"""
Object for running commands.
"""

import subprocess
import tempfile
import logging
logger = logging.getLogger()

class ProcessError(Exception):
    "unlike subprocess, this includes stderr"
    def __init__(self, returncode, cmd, stderr, stdout=None):
        self.returncode = returncode
        self.cmd = tuple(cmd)
        self.stderr = stderr
        self.stdout = stdout
        msg = " ".join(self.cmd) + " exited " + str(self.returncode)
        if self.stderr is not None:
            msg += ": " + self.stderr
        Exception.__init__(self, msg)


class Pipeline2Exception(Exception):
    def __init__(self, except1, except2):
        "either can be null"
        self.except1 = except1
        self.except2 = except2
        msgs = [str(except1) if except1 is not None else "",
                str(except2) if except1 is not None else ""]
        Exception.__init__(self, "\n".join(msgs))

class AsyncProc(object):
    def __init__(self, cmd, stdin=None, stdout=None):
        self.cmd = cmd
        self.stderrFh = tempfile.NamedTemporaryFile(prefix="zfszipper")
        self.proc = subprocess.Popen(cmd, stdin=stdin, stdout=stdout, stderr=self.stderrFh)

    def waitNoThrow(self):
        "return (stderr, None) or (stderr, exception) on error, logs errors"
        try:
            code = self.proc.wait()
            if code != 0:
                self.stderrFh.seek(0)
                raise ProcessError(code, self.cmd, self.stderrFh.read())
            self.stderrFh.seek(0)
            return (self.stderrFh.read(), None)
        except Exception, ex:
            self.stderrFh.seek(0)
            stderr = self.stderrFh.read()
            logger.exception("failed: " + " " .join(self.cmd) + " got " + stderr)
            return (stderr, ex)
        finally:
            self.stderrFh.close()

class CmdRunner(object):
    def _logCmd(self, cmd):
        logger.debug("run: " + " ".join(cmd))

    def _run(self, cmd):
        # check_output doesn't return stderr in message
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            raise ProcessError(process.returncode, cmd, stderr)
        return stdout

    def call(self, cmd):
        "return list of output lines"
        self._logCmd(cmd)
        try:
            lines = self._run(cmd).splitlines()
        except Exception:
            logger.exception("command failed:" + " " .join(cmd))
            raise
        return lines

    def callTabSplit(self, cmd):
        "return list of lines, split by row"
        lines = self.call(cmd)
        return [l.split("\t") for l in lines]

    def pipeline2(self, cmd1, cmd2):
        """pipeline two processes, capturing stderr, either throw in exception or
        returned as (stderr1, strderr2)"""
        self._logCmd(cmd1 + ["|"] + cmd2)
        p1 = AsyncProc(cmd1, stdout=subprocess.PIPE)
        p2 = AsyncProc(cmd2, stdin=p1.proc.stdout)
        p1.proc.stdout.close()  # Allow process to receive a SIGPIPE if other process exits
        stderr1, ex1 = p1.waitNoThrow()
        stderr2, ex2 = p2.waitNoThrow()
        if (ex1 is not None) or (ex2 is not None):
            raise Pipeline2Exception(ex1, ex2)
        return (stderr1, stderr2)
