"""
support for tests on ZFS in FreeBSD vnode devices
"""
import os
from collections import namedtuple
from testops import *

MdDevices = namedtuple("MdDevices", ("device", "unit", "file"))
def mdconfigList():
    "mdconfig list exits 255 for some reason on success", 
    cmd = ["mdconfig", "-lv"]
    results = callCmdAllResults(cmd)
    if results.returncode not in (0, 255):
        raise subprocess.CalledProcessError(results.returncode, cmd, results.stderr)
    devs = []
    for row in  [l.split("\t") for l in results.stdout.splitlines()]:
        # md10	vnode	   64M	/var/tmp/zfszipper-tests/dev/unit10
        devs.append(MdDevices(row[0], row[0][2:], row[3]))
    return tuple(devs)

class _ZfsVnodePool(object):
    """zfs pool in a vnode disk on a file"""

    ZfsFs = namedtuple("ZfsFs", ("fileSystemName", "mountPoint"))
    def __init__(self, testRootDir, poolName, unitNum, otherFileSystems=[]):
        self.poolName = poolName
        self.unitNum = unitNum
        self.unitDev = "/dev/md"+str(unitNum)
        self.devFile = testRootDir + "/dev/unit" + str(unitNum)
        self.mntDir = testRootDir + "/mnt"
        self.sizeMb = 64
        # first is main pool fs
        self.fileSystems = tuple([self.ZfsFs(self.poolName, self.mntDir+"/"+self.poolName)]
                                 + [self.ZfsFs(fs, self.mntDir+"/"+fs) for fs in otherFileSystems])

    def __createVnodeDisk(self):
        ensureFileDir(self.devFile)
        ensureDir(self.mntDir)
        if os.path.exists(self.devFile):
            os.unlink(self.devFile)
        runCmd(["dd", "if=/dev/zero", "of="+self.devFile, "bs=1m", "count="+str(self.sizeMb)])
        runCmd(["mdconfig", "-a", "-t", "vnode", "-f",  self.devFile, "-u", str(self.unitNum)])

    def getFileSystem(self, fileSystemName):
        for zfsFs in self.fileSystems:
            if zfsFs.fileSystemName == fileSystemName:
                return zfsFs
        raise Exception("can't find filesystem " + fileSystemName)
    
    def setup(self):
        self.__createVnodeDisk()
        zfsPoolCreate(self.fileSystems[0].mountPoint, self.poolName, self.unitDev)
        for zfsFs in self.fileSystems[1:]:
            zfsFileSystemCreate(zfsFs.fileSystemName)

_nextUnitNumber = 10
def zfsVirtualCreatePool(testRootDir, poolName, otherFileSystems=[]):
    unitNum = _nextUnitNumber
    _nextUnitNumber += 1
    return _ZfsVnodePool(testRootDir, poolName, unitNum, otherFileSystems)

def _findTestDevices(testRootDir):
    return tuple([dev for dev in  mdconfigList() if dev.file.startswith(testRootDir)])

def _destroyTestDevices(dev):
    runCmd(["mdconfig", "-d", "-u", dev.unit, "-o", "force"])
    os.unlink(dev.file)

def zfsVirtualCleanup(testRootDir, poolNamePrefix):
    testRootDir = os.path.normpath(testRootDir)
    for testPool in zfsFindTestPools(poolNamePrefix, testRootDir):
        zfsPoolDestroy(testPool, force=True)
    for dev in _findTestDevices(testRootDir):
        _destroyTestDevices(dev)
