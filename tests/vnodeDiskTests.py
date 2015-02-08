"""
Tests that run on FreeBSD with ZFS in vnode file systems.  Must run as root, sadly.
"""
import os, sys, argparse
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
    testSourceFs2Files = ("fs2/one", "fs2/two", "fs2/three")

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
            fh.write(contents)

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
            cmd.append("--stderrLogLevel="+self.zipperLogLevel)    
        runCmd(cmd)

    def runTest(self):
        sourcePool, backupPool = self.__testInit()
        configPy = writeConfigPy(self.testEtcDir, self.configPyCode)
        self.__writeTestFiles(sourcePool, self.testSourceFs1, self.testSourceFs1Files)
        self.__runZfsZipper(configPy, full=True, allowOverwrite=False)
        
def parseCommand():
    usage="""%prog [options] test|cleanup
    runs tests or do a cleanup
    """
    parser = argparse.ArgumentParser(description=usage)
    parser.add_argument("action",
                        help="""test to run tests, or cleanup to cleanup failed tests""")
    args = parser.parse_args()
    if args.action not in ("test", "cleanup"):
        parser.error("expected on of test or cleanup, got %s"% args.action)
    if os.geteuid() != 0:
        parser.error("must be run as root")
    return args

args = parseCommand()
# ensure subprocess can find library
os.environ["PYTHONPATH"] = "..:" + os.environ.get("PYTHONPATH", "")
if args.action == "test":
    VNodeDiskTests().runTest()
else:
    VNodeDiskTests.cleanup()
