"""
support for tests on ZFS in FreeBSD vnode devices
"""
import os, sys, argparse
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

class Cleanup(object):
    def __init__(self, testRootDir, poolNamePrefix):
        testRootDir = os.path.normpath(testRootDir)
        self.testPools = self.__findTestPools(poolNamePrefix, testRootDir)
        self.testDevices = self.__findTestDevices(testRootDir)
        
    def __findTestPools(self, poolNamePrefix, testRootDir):
        # every paranoid checking
        testFileSystems = [fs for fs in runCmdTabSplit(["zfs", "list", "-H", "-o", "name,mountpoint", "-t", "filesystem"])
                           if fs[0].startswith(poolNamePrefix) and fs[1].startswith(testRootDir)]
        return frozenset([fs[0].split("/")[0] for fs in testFileSystems])

    def __findTestDevices(self, testRootDir):
        return tuple([dev for dev in  mdconfigList() if dev.file.startswith(testRootDir)])

    def __cleanupTestPool(self, poolName):
        runCmd(["zpool", "destroy", "-f", poolName])

    def __cleanupTestDevices(self, dev):
        runCmd(["mdconfig", "-d", "-u", dev.unit])
        os.unlink(dev.file)

    def cleanup(self):
        for testPool in self.testPools:
            self.__cleanupTestPool(testPool)
        for testDevice in self.testDevices:
            self.__cleanupTestDevices(testDevice)
    
class ZfsVnodePool(object):
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

    def __destroyZfsPool(self):
        runCmd(["zpool", "destroy", self.poolName])

    def __createZfsPool(self, zfsFs):
        if self.poolName in runCmd(["zpool", "list", "-H", "-o", "name"]):
            self.__destroyZfsPool()
        runCmd(["zpool", "create", "-m", zfsFs.mountPoint, self.poolName, self.unitDev])

    def getFileSystem(self, fileSystemName):
        for zfsFs in self.fileSystems:
            if zfsFs.fileSystemName == fileSystemName:
                return zfsFs
        raise Exception("can't find filesystem " + fileSystemName)
    
    def setup(self):
        self.__createVnodeDisk()
        self.__createZfsPool(self.fileSystems[0])
        for zfsFs in self.fileSystems[1:]:
            runCmd(["zfs", "create", zfsFs.fileSystemName])

