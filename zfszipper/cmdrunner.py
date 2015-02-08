"""
Object for running commands.
"""

import subprocess, tempfile
import logging
logger = logging.getLogger()

class Pipeline2Exception(Exception):
    def __init__(self, except1, except2):
        "either can be null"
        self.except1 = except1
        self.except2 = except2
        msgs = [str(except1) if except1 != None else "",
                str(except2) if except1 != None else ""]
        Exception.__init__(self, msgs.join("\n"))

class AsyncProc(object):
    def __init__(self, cmd, stdin=None, stdout=None):
        self.cmd = cmd
        self.stderrFh = tempfile.NamedTemporaryFile(delete=False)
        self.proc = subprocess.Popen(cmd, stdin=stdin, stdout=stdout, stderr=self.stderrFh)

    def waitNoThrow(self):
        "return (stderr, None) or (stderr, exception) on error, logs errors"
        try:
            code = self.proc.wait()
            if code != 0:
                raise subprocess.CalledProcessError(code, self.cmd, self.stderrFh.read())
            self.stderrFh.seek(0)
            return (self.stderrFh.read(), None)
        except Exception, ex:
            self.stderrFh.seek(0)
            stderr = self.stderrFh.read()
            logger.exception("command fails:" + " " .join(self.cmd) + " got " + stderr)
            return (stderr, ex)
        finally:
            self.stderrFh.close()
        
        
class CmdRunner(object):
    def __logCmd(self, cmd):
        logger.debug("run: " + " ".join(cmd) + "\n")

    def run(self, cmd):
        "execute command, not output returned"
        self.__logCmd(cmd)
        try:
            subprocess.check_call(cmd)
        except Exception, ex:
            logger.exception("command failed:" + " " .join(cmd))
            raise

    def call(self, cmd):
        "return list of output lines"
        self.__logCmd(cmd)
        try:
            lines = subprocess.check_output(cmd).splitlines()
        except Exception, ex:
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
        self.__logCmd(cmd1 + ["|"] + cmd2)
        p1 = AsyncProc(cmd1, stdout=subprocess.PIPE)
        p2 = AsyncProc(cmd2, stdin=p1.proc.stdout)
        p1.proc.stdout.close()  # Allow process to receive a SIGPIPE if other process exits
        stderr1, ex1 = p1.waitNoThrow()
        stderr2, ex2 = p2.waitNoThrow()
        if (ex1 != None) or (ex2 != None):
            raise Pipeline2Exception(ex1, ex2)
        return (stderr1, stderr2)
