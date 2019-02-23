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
from testops import ensureDir, runCmd, deleteFiles
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
    testSourceFs1 = testSourcePoolName
    testSourceFs2 = testSourcePoolName + "/fs2"
    testBackupPoolAName = testPoolPrefix + "_backupA"
    testBackupPoolBName = testPoolPrefix + "_backupB"
    testRootDir = "/var/tmp/zfszipper_tests"
    testEtcDir = testRootDir + "/etc"
    testVarDir = testRootDir + "/var"
    testLockFile = testVarDir + "/zfszipper.lock"
    testRecordPat = testVarDir + "/zfszipper.%Y-%m.record.tsv"

    testSourceFs1Files = ("one", "two", "three")
    testSourceFs1Files2 = ("six", "seven", "eight")
    testSourceFs2Files = ("one1", "two2", "three3")
    testSourceFs2Files2 = ("six6", "seven7", "eight8")

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
        deleteFiles(VirtualDiskTests.testVarDir + "/*")
        zfsVirtualCleanup(VirtualDiskTests.testRootDir, VirtualDiskTests.testPoolPrefix)

    def _testInit(self):
        self.cleanup()
        ensureDir(self.testVarDir)

    def _createSourcePool(self):
        sourcePool = zfsVirtualCreatePool(self.testRootDir, self.testSourcePoolName, [self.testSourceFs2])
        sourcePool.setup()
        sourcePool.createFileSystems()
        return sourcePool

    def _createBackupPool(self, testBackupPoolName):
        backupPool = zfsVirtualCreatePool(self.testRootDir, testBackupPoolName)
        backupPool.setup()
        backupPool.createFileSystems()
        return backupPool

    def _writeFile(self, path, contents):
        with open(path, "w") as fh:
            fh.write(contents + "\n")

    def _writeTestFiles(self, pool, fileSystem, relFileNames, contentFunction=lambda x: x):
        mountPoint = pool.getFileSystem(fileSystem).mountPoint
        for relFileName in relFileNames:
            self._writeFile(mountPoint + "/" + relFileName, contentFunction(relFileName))

    def _runZfsZipper(self, configPy, full, allowOverwrite=False, backupSet=None, sourceFileSystems=None):
        cmd = ["../sbin/zfs-zipper", configPy]
        if full:
            cmd.append("--full")
        if allowOverwrite:
            cmd.append("--allowOverwrite")
        if backupSet is not None:
            cmd.append("--backupSet=" + backupSet)
        if sourceFileSystems is not None:
            cmd.extend(["--sourceFileSystem=" + fs for fs in sourceFileSystems])
        if self.zipperLogLevel is not None:
            cmd.append("--verboseLevel=" + self.zipperLogLevel)
        runCmd(cmd)

    @staticmethod
    def _upcaseFunc(x):
        return x.upper()

    def _test1Full1(self, sourcePool, backupPool, configPy):
        self._writeTestFiles(sourcePool, self.testSourceFs1, self.testSourceFs1Files)
        self._writeTestFiles(sourcePool, self.testSourceFs2, self.testSourceFs2Files)
        self._runZfsZipper(configPy, full=True, allowOverwrite=False)

    def _test1Incr1(self, sourcePool, backupPool, configPy):
        self._writeTestFiles(sourcePool, self.testSourceFs1, self.testSourceFs1Files[1:2], self._upcaseFunc)
        self._writeTestFiles(sourcePool, self.testSourceFs2, self.testSourceFs2Files[0:1], self._upcaseFunc)
        self._runZfsZipper(configPy, full=False, allowOverwrite=False)

    def _test1Incr2(self, sourcePool, backupPool, configPy):
        self._writeTestFiles(sourcePool, self.testSourceFs1, self.testSourceFs1Files[0:2], self._upcaseFunc)
        self._writeTestFiles(sourcePool, self.testSourceFs2, self.testSourceFs2Files[2:2], self._upcaseFunc)
        self._writeTestFiles(sourcePool, self.testSourceFs1, self.testSourceFs1Files2)
        self._writeTestFiles(sourcePool, self.testSourceFs2, self.testSourceFs2Files2)
        # try restriction arguments
        self._runZfsZipper(configPy, full=False, allowOverwrite=False, backupSet=self.testBackupSetName, sourceFileSystems=[self.testSourceFs1, self.testSourceFs2])

    def _test1FullOverwriteFail(self, sourcePool, backupPool, configPy):
        ok = False
        try:
            self._runZfsZipper(configPy, full=True, allowOverwrite=False)
            ok = True
        except Exception, ex:
            expectMsg = "zfszipper_test_source to zfszipper_test_backupA/zfszipper_test_source: full backup snapshots exists and overwrite not specified"
            if ex.message.find(expectMsg) < 0:
                raise Exception("expected error with message containing \"%s\", got \"%s\"" % (expectMsg, str(ex)))
        if ok:
            raise Exception("Excepted failure, didn't get exception")

    def runTest1(self):
        self._testInit()
        sourcePool = self._createSourcePool()
        backupPool = self._createBackupPool(self.testBackupPoolAName)
        configPy = writeConfigPy(self.testEtcDir, self.configPyCode)
        self._test1Full1(sourcePool, backupPool, configPy)

        # FIXME:  want to test auto-import, however if export test_backup, then import
        # with
        #   sudo zpool import -d /var/tmp/zfszipper_tests/dev/
        #   gets error: zfszipper_test_backup.dmg  UNAVAIL  cannot open
        # backupPool.exportPool()
        self._test1Incr1(sourcePool, backupPool, configPy)
        self._test1Incr2(sourcePool, backupPool, configPy)
        self._test1FullOverwriteFail(sourcePool, backupPool, configPy)
        self.cleanup()


def parseCommand():
    usage = """%prog [options] test|clean
    runs tests or do a cleanup
    """
    parser = argparse.ArgumentParser(description=usage)
    parser.add_argument("action",
                        help="""test to run tests, or clean to cleanup failed tests""")
    args = parser.parse_args()
    if args.action not in ("test", "clean"):
        parser.error("expected on of test or clean, got %s" % args.action)
    if os.geteuid() != 0:
        parser.error("must be run as root")
    return args

args = parseCommand()
# ensure subprocess can find library
os.environ["PYTHONPATH"] = "..:" + os.environ.get("PYTHONPATH", "")
if args.action == "test":
    VirtualDiskTests().runTest1()
else:
    VirtualDiskTests.cleanup()
