"""
common functions used by various tests.
"""
import os
import sys
import subprocess
import errno
from collections import namedtuple
from zfszipper.cmdrunner import ProcessError, stdflush

def ensureDir(dir):
    """Ensure that a directory exists, creating it (and parents) if needed."""
    try:
        os.makedirs(dir)
    except OSError as ex:
        if ex.errno != errno.EEXIST:
            raise

def ensureFileDir(fname):
    """Ensure that the directory for a file exists, creating it (and parents) if needed.
    Returns the directory path"""
    dir = os.path.dirname(fname)
    if len(dir) > 0:
        ensureDir(dir)
        return dir
    else:
        return "."

def runCmdStr(cmd, encoding="utf-8"):
    sys.stderr.write("run: " + " ".join(cmd) + "\n")
    stdflush()
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding=encoding)
    stdout, stderr = process.communicate()
    stdflush()
    if isinstance(stderr, bytes):
        stderr = stderr.decode(errors='replace')
    sys.stderr.write(stderr)
    if process.returncode != 0:
        raise ProcessError(process.returncode, cmd, stderr)
    return stdout

def runCmd(cmd, encoding="utf-8"):
    return runCmdStr(cmd, encoding).splitlines()

def runCmdTabSplit(cmd, encoding="utf-8"):
    return [l.split("\t") for l in runCmd(cmd, encoding)]

CmdResults = namedtuple("CmdResults", ("returncode", "stdout", "stderr"))
def callCmdAllResults(cmd):
    "return CmdResults object"
    sys.stderr.write("run: " + " ".join(cmd) + "\n")
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    return CmdResults(p.returncode, stdout, stderr)

def zfsFindTestPools(poolNamePrefix, testRootDir):
    "run zfs to get a set of test pool names"
    # ever-paranoid checking
    testFileSystems = [fs for fs in runCmdTabSplit(["zfs", "list", "-H", "-o", "name,mountpoint", "-t", "filesystem"])
                       if fs[0].startswith(poolNamePrefix) and fs[1].startswith(testRootDir)]
    return frozenset([fs[0].split("/")[0] for fs in testFileSystems])

def zfsPoolDestroy(poolName, *, force=False):
    cmd = ["zpool", "destroy"]
    if force:
        cmd.append("-f")
    cmd.append(poolName)
    runCmd(cmd)

def zfsPoolCreate(mountPoint, poolName, device, *, force=False):
    if poolName in runCmd(["zpool", "list", "-H", "-o", "name"]):
        zfsPoolDestroy(poolName)
    cmd = ["zpool", "create"]
    if force:
        cmd.append('-f')
    cmd += ["-m", mountPoint, poolName, device]
    runCmd(cmd)
    runCmd(["zfs", "set", "atime=off", poolName])

def zfsPoolExport(poolName):
    runCmd(["zpool", "export", "-f", poolName])

def zfsPoolImport(poolName):
    runCmd(["zpool", "import", poolName])

def zfsFileSystemCreate(fileSystemName):
    runCmd(["zfs", "create", fileSystemName])
