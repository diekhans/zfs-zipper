"""
Tests that run on FreeBSD with ZFS in vnode file systems.  Must run as root, sadly.
"""
import os, sys, argparse, subprocess
sys.path.insert(0, os.path.normpath(os.path.dirname(sys.argv[0])) + "/../lib/zfs-zipper")
from collections import namedtuple
from testops import *
from vnodeZfs import *

def writeConfigPy(testEtcDir, codeStr):
    ensureDir(testEtcDir)
    configPy = testEtcDir + "/config.py"
    with open(configPy, "w") as fh:
        fh.write("# GENERATED DO NOT EDIT\n")
        fh.write("from zfszipper.config import *\n")
        fh.write(codeStr + "\n")
    return configPy

class VNodeDiskTests(object):
    zipperLogLevel = "debug"
    testPoolPrefix = "zfszipper_test"
    testBackupSetName = "testBackupSet"
    testSourcePool = testPoolPrefix + "_source"
    testSourceFs1 = testSourcePool
    testSourceFs2 = testSourcePool + "/fs2"
    testBackupPool = testPoolPrefix + "_backup"
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
backupSetConf = BackupSetConf("%(testBackupSetName)s", ["%(testSourceFs1)s","%(testSourceFs2)s"],
                             [BackupPoolConf("%(testBackupPool)s")])
config = BackupConf([backupSetConf], lockFile="%(testLockFile)s", recordFilePattern="%(testRecordPat)s")
""" % vars()

    @staticmethod
    def cleanup():
        deleteFiles(VNodeDiskTests.testVarDir+"/*")
        Cleanup(VNodeDiskTests.testRootDir, VNodeDiskTests.testPoolPrefix).cleanup()        
        
    def __testInit(self):
        self.cleanup()
        ensureDir(self.testVarDir)
        sourcePool = ZfsVnodePool(self.testRootDir, self.testSourcePool, 10, [self.testSourceFs2])
        backupPool = ZfsVnodePool(self.testRootDir, self.testBackupPool, 11)
        sourcePool.setup()
        backupPool.setup()
        return (sourcePool, backupPool)

    def __writeFile(self, path, contents):
        with open(path, "w") as fh:
            fh.write(contents + "\n")

    def __writeTestFiles(self, pool, fileSystem, relFileNames, contentFunction=lambda x:x):
        mountPoint = pool.getFileSystem(fileSystem).mountPoint
        for relFileName in relFileNames:
            self.__writeFile(mountPoint+"/"+relFileName, contentFunction(relFileName))

    def __runZfsZipper(self, configPy, full, allowOverwrite=False):
        cmd = ["../bin/zfs-zipper", configPy]
        if full:
            cmd.append("--full")
        if allowOverwrite:
            cmd.append("--allowOverwrite")
        if self.zipperLogLevel != None:
            cmd.append("--verboseLevel="+self.zipperLogLevel)    
        runCmd(cmd)

    @staticmethod
    def __upcaseFunc(x):
        return x.upper()

    def __test1Full1(self, sourcePool, backupPool, configPy):
        self.__writeTestFiles(sourcePool, self.testSourceFs1, self.testSourceFs1Files)
        self.__writeTestFiles(sourcePool, self.testSourceFs2, self.testSourceFs2Files)
        self.__runZfsZipper(configPy, full=True, allowOverwrite=False)

    def __test1Incr1(self, sourcePool, backupPool, configPy):
        self.__writeTestFiles(sourcePool, self.testSourceFs1, self.testSourceFs1Files[1:2], self.__upcaseFunc)
        self.__writeTestFiles(sourcePool, self.testSourceFs2, self.testSourceFs2Files[0:1], self.__upcaseFunc)
        self.__runZfsZipper(configPy, full=False, allowOverwrite=False)

    def __test1Incr2(self, sourcePool, backupPool, configPy):
        self.__writeTestFiles(sourcePool, self.testSourceFs1, self.testSourceFs1Files[0:2], self.__upcaseFunc)
        self.__writeTestFiles(sourcePool, self.testSourceFs2, self.testSourceFs2Files[2:2], self.__upcaseFunc)
        self.__writeTestFiles(sourcePool, self.testSourceFs1, self.testSourceFs1Files2)
        self.__writeTestFiles(sourcePool, self.testSourceFs2, self.testSourceFs2Files2)
        self.__runZfsZipper(configPy, full=False, allowOverwrite=False)

    def __test1FullOverwriteFail(self, sourcePool, backupPool, configPy):
        ok = False
        try:
            self.__runZfsZipper(configPy, full=True, allowOverwrite=False)
            ok = True
        except Exception, ex:
            expectMsg = "zfszipper_test_source to zfszipper_test_backup/zfszipper_test_source: full backup snapshots exists and overwrite not specified"
            if ex.message.find(expectMsg) < 0:
                raise Exception("expected error with message containing \"%s\", got \"%s\"" % (expectMsg, str(ex)))
        if ok:
            raise Exception("Excepted failure, didn't get exception")
        
    def runTest1(self):
        sourcePool, backupPool = self.__testInit()
        configPy = writeConfigPy(self.testEtcDir, self.configPyCode)
        self.__test1Full1(sourcePool, backupPool, configPy)
        self.__test1Incr1(sourcePool, backupPool, configPy)
        self.__test1Incr2(sourcePool, backupPool, configPy)
        self.__test1FullOverwriteFail(sourcePool, backupPool, configPy)

def parseCommand():
    usage="""%prog [options] test|clean
    runs tests or do a cleanup
    """
    parser = argparse.ArgumentParser(description=usage)
    parser.add_argument("action",
                        help="""test to run tests, or clean to cleanup failed tests""")
    args = parser.parse_args()
    if args.action not in ("test", "clean"):
        parser.error("expected on of test or clean, got %s"% args.action)
    if os.geteuid() != 0:
        parser.error("must be run as root")
    return args

args = parseCommand()
# ensure subprocess can find library
os.environ["PYTHONPATH"] = "..:" + os.environ.get("PYTHONPATH", "")
if args.action == "test":
    VNodeDiskTests().runTest1()
else:
    VNodeDiskTests.cleanup()
