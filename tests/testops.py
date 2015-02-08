"""
common functions used by various tests.
"""
import os, sys, subprocess, errno
from glob import glob
from collections import namedtuple

def ensureDir(dir):
    """Ensure that a directory exists, creating it (and parents) if needed."""
    try: 
        os.makedirs(dir)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise e

def ensureFileDir(fname):
    """Ensure that the directory for a file exists, creating it (and parents) if needed.
    Returns the directory path"""
    dir = os.path.dirname(fname)
    if len(dir) > 0:
        ensureDir(dir)
        return dir
    else:
        return "."

def deleteFiles(globPat):
    for f in glob(globPat):
        os.unlink(f)

def runCmd(cmd):
    sys.stderr.write("run: " + " ".join(cmd) + "\n")
    return subprocess.check_output(cmd).splitlines()

def runCmdTabSplit(cmd):
    sys.stderr.write("run: " + " ".join(cmd) + "\n")
    return [l.split("\t") for l in subprocess.check_output(cmd).splitlines()]

CmdResults = namedtuple("CmdResults", ("returncode", "stdout", "stderr"))
def callCmdAllResults(cmd):
    "return CmdResults object"
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = p.communicate()
    p.wait()
    return CmdResults(p.returncode, out[0], out[1])

