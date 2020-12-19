"""
Tests that run on FreeBSD with ZFS in vnode file systems or OS/X
on a virtual disk.  Must run as root, sadly.
"""
import os
import sys
import argparse
import logging
logger = logging.getLogger()
sys.path.insert(0, os.path.normpath(os.path.dirname(sys.argv[0])) + "/../lib/zfs-zipper")
from testops import ensureDir, runCmd
if os.uname()[0] == 'Darwin':
    from macVirtualZfs import zfsVirtualCreatePool, zfsVirtualCleanup
else:
    from freeBsdVirtualZfs import zfsVirtualCreatePool, zfsVirtualCleanup

def writeConfigPy(testEtcDir, codeStr):
    ensureDir(testEtcDir)
    configPy = testEtcDir + "/config.py"
    with open(configPy, "w") as fh:
        fh.write("# GENERATED DO NOT EDIT\n")
        fh.write("from zfszipper.config import *\n")
        fh.write(codeStr + "\n")
    return configPy

class VirtualDiskTests(object):
    zipperLogLevel = "debug"
    testPoolPrefix = "zfszipper_test"
    testBackupSetName = "testBackupSet"
    testSourcePoolName = testPoolPrefix + "_source"
    testSourceFs1 = testSourcePoolName + "/fs1"
    testSourceFs2 = testSourcePoolName + "/fs2"
    testBackupPoolAName = testPoolPrefix + "_backupA"
    testBackupPoolBName = testPoolPrefix + "_backupB"
    testRootDir = "/var/tmp/zfszipper_tests"
    testEtcDir = testRootDir + "/etc"
    testVarDir = testRootDir + "/var"
    testLockFile = testVarDir + "/zfszipper.lock"
    testRecordPat = testVarDir + "/zfszipper.%Y-%m.record.tsv"

    testSourceFs1Files = ("one", "two", "three", "four")
    testSourceFs1Files2 = ("six", "seven", "eight", "nine")
    testSourceFs2Files = ("one1", "two2", "three3", "four4")
    testSourceFs2Files2 = ("six6", "seven7", "eight8", "nine9")

    configPyCode = """
backupSetConf = BackupSetConf("%(testBackupSetName)s",
                             ["%(testSourceFs1)s","%(testSourceFs2)s"],
                             [BackupPoolConf("%(testBackupPoolAName)s"),
                              BackupPoolConf("%(testBackupPoolBName)s"),
                             ])
config = BackupConf([backupSetConf],
                    lockFile="%(testLockFile)s",
                    recordFilePattern="%(testRecordPat)s")
""" % vars()

    @staticmethod
    def cleanup():
        zfsVirtualCleanup(VirtualDiskTests.testRootDir, VirtualDiskTests.testPoolPrefix)
        runCmd(['rm', '-rf', VirtualDiskTests.testRootDir])

    def _testInit(self):
        self.cleanup()
        ensureDir(self.testVarDir)

    def _createSourcePool(self):
        sourcePool = zfsVirtualCreatePool(self.testRootDir, self.testSourcePoolName, [self.testSourceFs1, self.testSourceFs2])
        sourcePool.setup()
        sourcePool.createFileSystems()
        return sourcePool

    def _createBackupPool(self, testBackupPoolName):
        backupPool = zfsVirtualCreatePool(self.testRootDir, testBackupPoolName)
        backupPool.setup()
        backupPool.createFileSystems()
        return backupPool

    def _writeFile(self, path, contents):
        "always appends so files an be modified"
        with open(path, "a") as fh:
            fh.write(contents + "\n")

    def _writeTestFiles(self, pool, fileSystem, relFileNames, contentFunction=lambda x: x):
        mountPoint = pool.getFileSystem(fileSystem).mountPoint
        for relFileName in relFileNames:
            self._writeFile(mountPoint + "/" + relFileName, contentFunction(relFileName))

    def _runZfsZipper(self, configPy, *, backupSet=None, sourceFileSystems=None, snapOnly=False):
        cmd = ["../sbin/zfs-zipper", "--conf={}".format(configPy)]
        if sourceFileSystems is not None:
            cmd.extend(["--sourceFileSystem=" + fs for fs in sourceFileSystems])
        if snapOnly:
            cmd.append("--snapOnly")
        if self.zipperLogLevel is not None:
            cmd.append("--verboseLevel=" + self.zipperLogLevel)
        if backupSet is not None:
            cmd.append(backupSet)
        runCmd(cmd)

    @staticmethod
    def _upcaseFunc(x):
        return x.upper()

    def _test1Full1(self, sourcePool, backupPool, configPy):
        self._writeTestFiles(sourcePool, self.testSourceFs1, self.testSourceFs1Files)
        self._writeTestFiles(sourcePool, self.testSourceFs2, self.testSourceFs2Files)
        self._runZfsZipper(configPy)

    def _test1Incr1(self, sourcePool, backupPool, configPy):
        self._writeTestFiles(sourcePool, self.testSourceFs1, self.testSourceFs1Files[1:2], self._upcaseFunc)
        self._writeTestFiles(sourcePool, self.testSourceFs2, self.testSourceFs2Files[0:1], self._upcaseFunc)
        self._runZfsZipper(configPy)

    def _test1Incr2(self, sourcePool, backupPool, configPy):
        self._writeTestFiles(sourcePool, self.testSourceFs1, self.testSourceFs1Files[0:2], self._upcaseFunc)
        self._writeTestFiles(sourcePool, self.testSourceFs2, self.testSourceFs2Files[2:2], self._upcaseFunc)
        self._writeTestFiles(sourcePool, self.testSourceFs1, self.testSourceFs1Files2)
        self._writeTestFiles(sourcePool, self.testSourceFs2, self.testSourceFs2Files2)
        # try restriction arguments
        self._runZfsZipper(configPy, backupSet=self.testBackupSetName, sourceFileSystems=[self.testSourceFs1, self.testSourceFs2])

    def _test1Incr3(self, sourcePool, configPy):
        self._writeTestFiles(sourcePool, self.testSourceFs1, self.testSourceFs1Files[0:2], self._upcaseFunc)
        self._writeTestFiles(sourcePool, self.testSourceFs2, self.testSourceFs2Files[2:2], self._upcaseFunc)
        self._writeTestFiles(sourcePool, self.testSourceFs1, self.testSourceFs1Files2)
        self._writeTestFiles(sourcePool, self.testSourceFs2, self.testSourceFs2Files2)
        # try with pool discovery
        self._runZfsZipper(configPy)

    def _test1Incr4SnapOnly(self, sourcePool, backupPool, configPy):
        self._writeTestFiles(sourcePool, self.testSourceFs1, self.testSourceFs1Files[1:3], self._upcaseFunc)
        self._writeTestFiles(sourcePool, self.testSourceFs2, self.testSourceFs2Files[0:2], self._upcaseFunc)
        self._runZfsZipper(configPy, snapOnly=True)

    def runTest1(self, noClean):
        # FIXME:  See to-do on testing autoimport
        self._testInit()
        sourcePool = self._createSourcePool()
        backupPoolA = self._createBackupPool(self.testBackupPoolAName)
        configPy = writeConfigPy(self.testEtcDir, self.configPyCode)

        # full blackup
        self._test1Full1(sourcePool, backupPoolA, configPy)

        # incrementals
        self._test1Incr1(sourcePool, backupPoolA, configPy)
        self._test1Incr2(sourcePool, backupPoolA, configPy)

        # pool discover
        self._test1Incr3(sourcePool, configPy)

        # just make snapshots
        self._test1Incr4SnapOnly(sourcePool, backupPoolA, configPy)

        if not noClean:
            self.cleanup()


def parseCommand():
    usage = """runs tests or do a cleanup
    """
    parser = argparse.ArgumentParser(description=usage)
    parser.add_argument("--noClean", default=False, action="store_true",
                        help="""Don't do cleanup after test run to allow inspection""")
    parser.add_argument("action", choices=("test", "clean"),
                        help="""test to run tests, or clean to cleanup failed tests""")
    args = parser.parse_args()
    if args.action not in ("test", "clean"):
        parser.error("expected on of test or clean, got {}".format(args.action))
    if os.geteuid() != 0:
        parser.error("must be run as root")
    return args

def main(args):
    # ensure subprocess can find library
    os.environ["PYTHONPATH"] = "..:" + os.environ.get("PYTHONPATH", "")
    if args.action == "test":
        VirtualDiskTests().runTest1(args.noClean)
    else:
        VirtualDiskTests.cleanup()

main(parseCommand())
