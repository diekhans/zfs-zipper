"""
support for tests on ZFS in Mac OS/X virtual devices
"""
import os
import os.path as osp
import re
import time
from collections import namedtuple
import plistlib
from testops import ensureDir, ensureFileDir, runCmd, runCmdStr
from testops import zfsPoolImport, zfsPoolExport, zfsPoolCreate, zfsPoolDestroy, zfsFindTestPools, zfsFileSystemCreate

def runCmdPlist(cmd):
    "returns parsed plist output"
    return plistlib.loads(runCmdStr(cmd, encoding=None))

VirtDevice = namedtuple("VirtDevice", ("device", "file"))

def virtualDevListParseDevice(entry):
    device = None
    # get top level device
    for sysEntity in entry['system-entities']:
        if re.match("^/dev/disk[0-9]+$", sysEntity['dev-entry']):
            device = sysEntity['dev-entry']
    if device is None:
        raise Exception("dev-entry not found: {}".format(entry))
    return device

def virtualDevListParse(entry):
    return VirtDevice(virtualDevListParseDevice(entry), entry['image-path'])

def virtualDevList():
    hdiList = runCmdPlist(["hdiutil", "info", "-plist"])
    devs = []
    for entry in hdiList['images']:
        devs.append(virtualDevListParse(entry))
    return tuple(devs)

class _ZfsVirtualPool(object):
    """zfs pool in a vnode disk on a file"""

    ZfsFs = namedtuple("ZfsFs", ("fileSystemName", "mountPoint"))

    def __init__(self, testRootDir, poolName, otherFileSystems=[]):
        self.poolName = poolName
        self.device = None
        self.mntDir = testRootDir + "/mnt"
        self.devFile = testRootDir + "/dev/" + poolName + ".dmg"
        self.sizeMb = 256
        # first is main pool fs
        self.fileSystems = tuple([self.ZfsFs(self.poolName, self.mntDir + "/" + self.poolName)]
                                 + [self.ZfsFs(fs, self.mntDir + "/" + fs) for fs in otherFileSystems])

    def _createVnodeDisk(self):
        ensureFileDir(self.devFile)
        ensureDir(self.mntDir)
        if osp.exists(self.devFile):
            os.unlink(self.devFile)
        # must force HFS+ as partitions work with the way we create ZFS
        runCmd(["hdiutil", "create", "-fs", "HFS+", "-size", str(self.sizeMb) + "m", self.devFile])
        attachPlist = runCmdPlist(["hdiutil", "attach", "-nomount", "-plist", self.devFile])
        self.device = virtualDevListParseDevice(attachPlist)

        # Trying to create a pool on a newly attached virtual device sometimes causes
        # EBUSY.  Appears a wait helps (yuck)
        time.sleep(1)

    def getFileSystem(self, fileSystemName):
        for zfsFs in self.fileSystems:
            if zfsFs.fileSystemName == fileSystemName:
                return zfsFs
        raise Exception("can't find filesystem " + fileSystemName)

    def setup(self):
        self._createVnodeDisk()
        zfsPoolCreate(self.fileSystems[0].mountPoint, self.poolName, self.device, force=True)

    def createFileSystems(self):
        for zfsFs in self.fileSystems[1:]:
            zfsFileSystemCreate(zfsFs.fileSystemName)

    def exportPool(self):
        zfsPoolExport(self.poolName)

    def importPool(self):
        zfsPoolImport(self.poolName)

def zfsVirtualCreatePool(testRootDir, poolName, otherFileSystems=[]):
    return _ZfsVirtualPool(testRootDir, poolName, otherFileSystems)

def _findTestDevices(testRootDir):
    return tuple([dev for dev in virtualDevList() if dev.file.startswith(testRootDir)])

def _destroyTestDevices(dev):
    runCmd(["hdiutil", "detach", dev.device])
    os.unlink(dev.file)

def zfsVirtualCleanup(testRootDir, poolNamePrefix):
    testRootDir = osp.normpath(testRootDir)
    for testPool in zfsFindTestPools(poolNamePrefix, testRootDir):
        zfsPoolDestroy(testPool, force=True)
    for dev in _findTestDevices(testRootDir):
        _destroyTestDevices(dev)
