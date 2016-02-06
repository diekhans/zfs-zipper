"""
common functions used by various tests.
"""
import os, sys, subprocess, errno
from glob import glob
from collections import namedtuple
from zfszipper.cmdrunner import ProcessError

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

def runCmdStr(cmd):
    sys.stderr.write("run: " + " ".join(cmd) + "\n")
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise ProcessError(process.returncode, cmd, stderr)
    return stdout

def runCmd(cmd):
    return runCmdStr(cmd).splitlines()

def runCmdTabSplit(cmd):
    return [l.split("\t") for l in runCmd(cmd)]

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

def zfsPoolDestroy(poolName, force=False):
    cmd = ["zpool", "destroy"]
    if force:
        cmd.append("-f")
    cmd.append(poolName)
    runCmd(cmd)

def zfsPoolCreate(mountPoint, poolName, device):
    if poolName in runCmd(["zpool", "list", "-H", "-o", "name"]):
        zfsPoolDestroy(poolName)
    runCmd(["zpool", "create", "-m", mountPoint, poolName, device])
    runCmd(["zfs", "set", "atime=off", poolName])

def zfsFileSystemCreate(fileSystemName):
    runCmd(["zfs", "create", fileSystemName])
