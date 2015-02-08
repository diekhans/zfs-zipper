"""
Test of zfs-zipper tests of library functions.
"""

import sys, unittest, tempfile
sys.path.insert(0, "..")
from zfszipper import backup
from zfszipper.backup import BackupSnapshot, BackupType, FsBackup, BackupError,  BackupSetBackup, BackupRecorder
from zfszipper.zfs import ZfsPool, ZfsFileSystem, ZfsSnapshot, ZfsPoolHealth
from zfszipper.config import *
from zfsMock import ZfsMock
from zfszipper.typeops import splitLinesToRows

import logging
logging.basicConfig(filename="/dev/null")

def fakeZfsFileSystem(fileSystemName, pool, mounted=True):
    "defaults parameters for easy fake construction"
    poolName = pool if isinstance(pool, str) else pool.name
    return ZfsFileSystem(fileSystemName, "/mnt/"+fileSystemName, mounted)

class GmtTimeFaker(object):
    "replaces currentGmtTimeStr function to produce a predictable set of time responses"
    
    def __init__(self, yearMonDay, hour=0, minute=0, sec=0):
        self.yearMonDay = yearMonDay
        self.hour = hour
        self.minute = minute
        self.sec = sec
        
    def __call__(self):
        timestr = "%sT%0.2d:%0.2d:%0.2d" % (self.yearMonDay, self.hour, self.minute, self.sec)
        self.sec += 1
        if self.sec >= 60:
            self.sec = 0
            self.minute += 1
        if self.minute >= 60:
            self.minute = self.sec = 0
            self.hour += 1
        return timestr

    @staticmethod
    def setTime(yearMonDay, hour=0, minute=0, sec=0):
        timer = GmtTimeFaker(yearMonDay, hour, minute, sec)
        backup.currentGmtTimeStrFunc = timer
        return timer
    
class TestBackupRecorder(BackupRecorder):
    "adds functionality for build on tests and cleaning up"

    def __init__(self, testId):
        fd, self.tmpTsv = tempfile.mkstemp(".tsv", "backup-test."+testId)
        os.close(fd)
        BackupRecorder.__init__(self, self.tmpTsv)

    def readLines(self):
        with open(self.tmpTsv) as fh:
            return splitLinesToRows(fh.read())
        
    def __del__(self):
        os.unlink(self.tmpTsv)
        self.close()  # ensure closed

class BackupSnapshotTests(unittest.TestCase):
    testPool = "swimming"
    testFs1 = "zztop/opt"
    testFs2 = "funpool1/zztop/opt"
    fullTestName = "zipper_1979-01-20T16:14:05_funbackset_full_funpool1"
    incrTestName = "zipper_2001-01-20T16:14:05_funbackset_incr"

    @staticmethod
    def __mkFsSnapshot(fs, ss):
        return fs + "@" + ss

    def testFullParse(self):
        ss = BackupSnapshot.createFromSnapshotName(self.fullTestName)
        self.assertEqual(str(ss), self.fullTestName)

    def testIncrParse(self):
        ss = BackupSnapshot.createFromSnapshotName(self.incrTestName)
        self.assertEqual(str(ss), self.incrTestName)

    def testFullParseFs(self):
        fsSSName = self.__mkFsSnapshot(self.testFs1, self.fullTestName)
        ss = BackupSnapshot.createFromSnapshotName(fsSSName)
        self.assertEqual(str(ss), fsSSName)

    def testIncrParseFs(self):
        fsSSName = self.__mkFsSnapshot(self.testFs1, self.incrTestName)
        ss = BackupSnapshot.createFromSnapshotName(fsSSName)
        self.assertEqual(str(ss), fsSSName)

    def testDropFsParse(self):
        fsSSName = self.__mkFsSnapshot(self.testFs1, self.incrTestName)
        ss = BackupSnapshot.createFromSnapshotName(fsSSName, dropFileSystem=True)
        self.assertEqual(str(ss), self.incrTestName)

    def testDropFsParse(self):
        fsSSName = self.__mkFsSnapshot(self.testFs1, self.incrTestName)
        ss = BackupSnapshot.createFromSnapshotName(fsSSName, dropFileSystem=True)
        self.assertEqual(str(ss), self.incrTestName)

    def testCurrentFull(self):
        ss = BackupSnapshot.createCurrent("someset", BackupSnapshot.full, "somepool", fakeZfsFileSystem("somefs", "somepool"))
        self.assertRegexpMatches(str(ss), "^somefs@zipper_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}_someset_full_somepool$")

    def testCurrentFull(self):
        ss = BackupSnapshot.createCurrent("someset", BackupType.full, "somepool", fakeZfsFileSystem("somefs", "somepool"))
        self.assertRegexpMatches(str(ss), "^somefs@zipper_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}_someset_full_somepool$")

    def testCurrentIncr(self):
        ss = BackupSnapshot.createCurrent("someset", BackupType.incr)
        self.assertRegexpMatches(str(ss), "^zipper_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}_someset_incr$")

    def testFromSnapshot(self):
        ss = BackupSnapshot.createFromSnapshotName(self.fullTestName)
        ss2 = BackupSnapshot.createFromSnapshot(ss)
        self.assertEqual(str(ss2), self.fullTestName)

    def testFromSnapshotSubstFS(self):
        fs1SSName = self.__mkFsSnapshot(self.testFs1, self.incrTestName)
        fs2SSName = self.__mkFsSnapshot(self.testFs2, self.incrTestName)
        ss = BackupSnapshot.createFromSnapshotName(fs1SSName)
        ss2 = BackupSnapshot.createFromSnapshot(ss, fileSystem=fakeZfsFileSystem(self.testFs2, self.testPool))
        self.assertEqual(str(ss2), fs2SSName)

class BackuperTests(unittest.TestCase):
    backupConf1 = BackupSetConf("testBackupSet",
                                [SourceFileSystemConf("srcPool1"),
                                 "srcPool1/srcPool1Fs2"],
                                [BackupPoolConf("backupPool1"),
                                 BackupPoolConf("backupPool2")])
    srcPool1 = ZfsPool("srcPool1", ZfsPoolHealth.ONLINE)
    srcPool1Fs1 = fakeZfsFileSystem("srcPool1", srcPool1)
    srcPool1Fs1StraySnapShots = (ZfsSnapshot("otherSnap1"), ZfsSnapshot("otherSnap2"))
    srcPool1Fs2 = fakeZfsFileSystem("srcPool1/srcPool1Fs2", srcPool1)

    backupPool1 = ZfsPool("backupPool1", ZfsPoolHealth.ONLINE)
    backupPool1Fs1 = fakeZfsFileSystem("backupPool1/srcPool1", backupPool1)
    backupPool1Fs2 = fakeZfsFileSystem("backupPool1/srcPool1/srcPool1Fs2", backupPool1)

    backupPool2 = ZfsPool("backupPool2", ZfsPoolHealth.ONLINE)
    backupPool2Fs1 = fakeZfsFileSystem("backupPool2/srcPool1", backupPool2)
    backupPool2Fs2 = fakeZfsFileSystem("backupPool2/srcPool1/srcPool1Fs2", backupPool2)

    backupPool1Off = ZfsPool("backupPool1", ZfsPoolHealth.ONLINE)
    backupPool1Fs1Off = fakeZfsFileSystem("backupPool1/srcPool1", backupPool1Off)
    backupPool1Fs2Off = fakeZfsFileSystem("backupPool1/srcPool1/srcPool1Fs2", backupPool1Off)

    backupPool2Off = ZfsPool("backupPool2", ZfsPoolHealth.OFFLINE)
    backupPool2Fs1Off = fakeZfsFileSystem("backupPool2/srcPool1", backupPool2Off)
    backupPool2Fs2Off = fakeZfsFileSystem("backupPool2/srcPool1/srcPool1Fs2", backupPool2Off)

    backupPool1Fs1Snapshots = (ZfsSnapshot('zipper_2015-01-01T17:30:34_testBackupSet_full_backupPool1'),
                               ZfsSnapshot('zipper_2015-02-01T17:30:34_testBackupSet_incr'),
                               ZfsSnapshot('zipper_2015-03-02T17:30:34_testBackupSet_incr'))
    backupPool1Fs2Snapshots = (ZfsSnapshot('zipper_2015-01-01T17:30:34_testBackupSet_full_backupPool1'),
                               ZfsSnapshot('zipper_2015-02-01T17:30:34_testBackupSet_incr'),
                               ZfsSnapshot('zipper_2015-03-02T17:30:34_testBackupSet_incr')) 
    backupPool2Fs1Snapshots = (ZfsSnapshot('zipper_2015-01-01T17:30:34_testBackupSet_full_backupPool2'),
                               ZfsSnapshot('zipper_2015-02-01T17:30:34_testBackupSet_incr'),
                               ZfsSnapshot('zipper_2015-03-02T17:30:34_testBackupSet_incr'))
    backupPool2Fs2Snapshots = (ZfsSnapshot('zipper_2015-01-01T17:30:34_testBackupSet_full_backupPool2'),
                               ZfsSnapshot('zipper_2015-02-01T17:30:34_testBackupSet_incr'),
                               ZfsSnapshot('zipper_2015-03-02T17:30:34_testBackupSet_incr')) 


    def __mkInitialZfs(self):
        zfsConf = ((self.srcPool1, ((self.srcPool1Fs1, self.srcPool1Fs1StraySnapShots),
                                    (self.srcPool1Fs2, ()))),
                   (self.backupPool1, ()))
        return ZfsMock(zfsConf)

    def __mkBackupPool1Zfs(self, sourceFs1Snapshots=(), sourceFs2Snapshots=(),
                           backupFs1Snapshots=None, backupFs2Snapshots=None):
        "zfs configured for backupPool1 cases.  snapshots default to the same"
        if backupFs1Snapshots == None:
            backupFs1Snapshots = sourceFs1Snapshots
        if backupFs2Snapshots == None:
            backupFs2Snapshots = sourceFs1Snapshots
        zfsConf = ((self.srcPool1, ((self.srcPool1Fs1, self.srcPool1Fs1StraySnapShots+sourceFs1Snapshots),
                                    (self.srcPool1Fs2, sourceFs2Snapshots))),
                   (self.backupPool1, ((self.backupPool1Fs1, backupFs1Snapshots),
                                       (self.backupPool1Fs2, backupFs2Snapshots))))
        return ZfsMock(zfsConf)
        
    def __mkBackupPool2Zfs(self, sourceFs1Snapshots=(), sourceFs2Snapshots=(),
                           backupFs1Snapshots=None, backupFs2Snapshots=None,
                           includeBackupFileSystems=True):
        "zfs configured for backupPool2 cases.  snapshots default to the same"
        if backupFs1Snapshots == None:
            backupFs1Snapshots = sourceFs1Snapshots
        if backupFs2Snapshots == None:
            backupFs2Snapshots = sourceFs1Snapshots
        zfsConf = ((self.srcPool1, ((self.srcPool1Fs1, sourceFs1Snapshots),
                                    (self.srcPool1Fs2, sourceFs2Snapshots))),)
        if includeBackupFileSystems:
            zfsConf += ((self.backupPool2, ((self.backupPool2Fs1, backupFs1Snapshots),
                                            (self.backupPool2Fs2, backupFs2Snapshots))),)
        return ZfsMock(zfsConf)
        
    def __setupFsBackup1(self, zfs, sourceFileSystemName, backupPool=backupPool1, allowOverwrite=False):
        return FsBackup(zfs, self.backupConf1,
                        zfs.getFileSystem(sourceFileSystemName),
                        backupPool, allowOverwrite)
    
    def __twoFsBackup(self, zfs, recorder, backupType, backupPool=backupPool1, allowOverwrite=False):
        fsBackup = self.__setupFsBackup1(zfs, "srcPool1", backupPool=backupPool, allowOverwrite=allowOverwrite)
        fsBackup.backup(recorder, backupType)
        fsBackup = self.__setupFsBackup1(zfs, "srcPool1/srcPool1Fs2", backupPool=backupPool, allowOverwrite=allowOverwrite)
        fsBackup.backup(recorder, backupType)
    
    def __assertActions(self, zfs, expected):
        for i in xrange(min(len(zfs.actions), len(expected))):
            self.assertEqual(zfs.actions[i], expected[i])
        self.assertEquals(len(zfs.actions), len(expected))

    def __assertRecorded(self, recorder, expected):
        "expected should not include header line"
        header = 'time\taction\tsrc1Snap\tsrc2Snap\tbackupSnap\tsize\texception\tinfo'
        expectedHdr = [header] + list(expected)

        lines = recorder.readLines()
        for i in xrange(min(len(lines), len(expectedHdr))):
            self.assertEquals(lines[i], expectedHdr[i])
        self.assertEquals(len(lines), len(expectedHdr))

        
    def testInitialFull(self):
        GmtTimeFaker.setTime("2001-01-01")
        zfs = self.__mkInitialZfs()
        zfs.addSendRecvInfos(((("full", "srcPool1@zipper_2015-02-01T20:23:37_testBackupSet_full_backupPool1", "50000"), ("size", "50000")),
                              (("full", "srcPool1/srcPool1Fs2@zipper_2015-02-01T20:23:37_testBackupSet_full_backupPool1", "50000"), ("size", "50000"))))
        recorder = TestBackupRecorder(self.id())
        self.__twoFsBackup(zfs, recorder, BackupType.full)
        self.__assertActions(zfs,
                             ['zfs snapshot srcPool1@zipper_2001-01-01T00:00:00_testBackupSet_full_backupPool1',
                              'zfs send -P srcPool1@zipper_2001-01-01T00:00:00_testBackupSet_full_backupPool1 | zfs receive backupPool1/srcPool1@zipper_2001-01-01T00:00:00_testBackupSet_full_backupPool1',
                              'zfs snapshot srcPool1/srcPool1Fs2@zipper_2001-01-01T00:00:02_testBackupSet_full_backupPool1',
                              'zfs send -P srcPool1/srcPool1Fs2@zipper_2001-01-01T00:00:02_testBackupSet_full_backupPool1 | zfs receive backupPool1/srcPool1/srcPool1Fs2@zipper_2001-01-01T00:00:02_testBackupSet_full_backupPool1'])
        self.__assertRecorded(recorder,
                              ['2001-01-01T00:00:01\tfull\tsrcPool1@zipper_2001-01-01T00:00:00_testBackupSet_full_backupPool1\t\tbackupPool1/srcPool1@zipper_2001-01-01T00:00:00_testBackupSet_full_backupPool1\t50000\t\t',
                               '2001-01-01T00:00:03\tfull\tsrcPool1/srcPool1Fs2@zipper_2001-01-01T00:00:02_testBackupSet_full_backupPool1\t\tbackupPool1/srcPool1/srcPool1Fs2@zipper_2001-01-01T00:00:02_testBackupSet_full_backupPool1\t50000\t\t'])
        del recorder
        
    def testIncr1(self):
        # 1st incr 
        GmtTimeFaker.setTime("2001-01-02")
        zfs = self.__mkBackupPool1Zfs(self.backupPool1Fs1Snapshots[0:1],
                                      self.backupPool1Fs2Snapshots[0:1])
        zfs.addSendRecvInfos(((("incremental", "zipper_2015-01-01T17:30:34_testBackupSet_full_backupPool1", "srcPool1@zipper_2015-01-01T17:30:34_testBackupSet_incr", "50000"), ("size", "50000")),
                              (("incremental", "zipper__testBackupSet_full_backupPool1", "srcPool1/srcPool1Fs2@zipper_2015-01-01T17:30:34_testBackupSet_incr", "50000"), ("size", "50000"))))
        recorder = TestBackupRecorder(self.id())
        self.__twoFsBackup(zfs, recorder, BackupType.incr)
        self.__assertActions(zfs,
                             ['zfs snapshot srcPool1@zipper_2001-01-02T00:00:00_testBackupSet_incr',
                              'zfs send -P -i zipper_2015-01-01T17:30:34_testBackupSet_full_backupPool1 srcPool1@zipper_2001-01-02T00:00:00_testBackupSet_incr | zfs receive backupPool1/srcPool1@zipper_2001-01-02T00:00:00_testBackupSet_incr',
                              'zfs snapshot srcPool1/srcPool1Fs2@zipper_2001-01-02T00:00:02_testBackupSet_incr',
                              'zfs send -P -i zipper_2015-01-01T17:30:34_testBackupSet_full_backupPool1 srcPool1/srcPool1Fs2@zipper_2001-01-02T00:00:02_testBackupSet_incr | zfs receive backupPool1/srcPool1/srcPool1Fs2@zipper_2001-01-02T00:00:02_testBackupSet_incr'])
        self.__assertRecorded(recorder,
                              ['2001-01-02T00:00:01\tincr\tzipper_2015-01-01T17:30:34_testBackupSet_full_backupPool1\tsrcPool1@zipper_2001-01-02T00:00:00_testBackupSet_incr\tbackupPool1/srcPool1@zipper_2001-01-02T00:00:00_testBackupSet_incr\tsrcPool1/srcPool1Fs2@zipper_2015-01-01T17:30:34_testBackupSet_incr\t\t',
                               '2001-01-02T00:00:03\tincr\tzipper_2015-01-01T17:30:34_testBackupSet_full_backupPool1\tsrcPool1/srcPool1Fs2@zipper_2001-01-02T00:00:02_testBackupSet_incr\tbackupPool1/srcPool1/srcPool1Fs2@zipper_2001-01-02T00:00:02_testBackupSet_incr\tsrcPool1@zipper_2015-01-01T17:30:34_testBackupSet_incr\t\t'])
        del recorder

    def testIncr2(self):
        # 2nd incr 
        GmtTimeFaker.setTime("1999-02-01")
        zfs = self.__mkBackupPool1Zfs(self.backupPool1Fs1Snapshots[0:2],
                                      self.backupPool1Fs2Snapshots[0:2])
        zfs.addSendRecvInfos(((("incremental", "zipper_2015-02-01T17:30:34_testBackupSet_incr", "srcPool1@zipper_2015-02-01T20:23:37_testBackupSet_incr", "50000"), ("size", "50000")),
                              (("incremental", "zipper_2015-02-01T17:30:34_testBackupSet_incr", "srcPool1/srcPool1Fs2@zipper_2015-02-01T20:23:37_testBackupSet_incr", "50000"), ("size", "50000"))))
        recorder = TestBackupRecorder(self.id())
        self.__twoFsBackup(zfs, recorder, BackupType.incr)
        self.__assertActions(zfs,
                             ['zfs snapshot srcPool1@zipper_1999-02-01T00:00:00_testBackupSet_incr',
                              'zfs send -P -i zipper_2015-02-01T17:30:34_testBackupSet_incr srcPool1@zipper_1999-02-01T00:00:00_testBackupSet_incr | zfs receive backupPool1/srcPool1@zipper_1999-02-01T00:00:00_testBackupSet_incr',
                              'zfs snapshot srcPool1/srcPool1Fs2@zipper_1999-02-01T00:00:02_testBackupSet_incr',
                              'zfs send -P -i zipper_2015-02-01T17:30:34_testBackupSet_incr srcPool1/srcPool1Fs2@zipper_1999-02-01T00:00:02_testBackupSet_incr | zfs receive backupPool1/srcPool1/srcPool1Fs2@zipper_1999-02-01T00:00:02_testBackupSet_incr'])
        self.__assertRecorded(recorder,
                              ['1999-02-01T00:00:01\tincr\tzipper_2015-02-01T17:30:34_testBackupSet_incr\tsrcPool1@zipper_1999-02-01T00:00:00_testBackupSet_incr\tbackupPool1/srcPool1@zipper_1999-02-01T00:00:00_testBackupSet_incr\tsrcPool1/srcPool1Fs2@zipper_2015-02-01T20:23:37_testBackupSet_incr\t\t',
                               '1999-02-01T00:00:03\tincr\tzipper_2015-02-01T17:30:34_testBackupSet_incr\tsrcPool1/srcPool1Fs2@zipper_1999-02-01T00:00:02_testBackupSet_incr\tbackupPool1/srcPool1/srcPool1Fs2@zipper_1999-02-01T00:00:02_testBackupSet_incr\tsrcPool1@zipper_2015-02-01T20:23:37_testBackupSet_incr\t\t'])
        del recorder

    def testFullFail(self):
        # full on top of increment should fail without force
        GmtTimeFaker.setTime("1962-02-01")
        zfs = self.__mkBackupPool1Zfs(self.backupPool1Fs1Snapshots[0:3],
                                      self.backupPool1Fs2Snapshots[0:3])
        recorder = TestBackupRecorder(self.id())
        with self.assertRaisesRegexp(BackupError, "^backup of srcPool1 to backupPool1/srcPool1: full backup snapshots exists and overwrite not specified$"):
            self.__twoFsBackup(zfs, recorder, BackupType.full)
        self.__assertRecorded(recorder, ['1962-02-01T00:00:00\terror\tsrcPool1\t\t\tBackupError\tbackup of srcPool1 to backupPool1/srcPool1: full backup snapshots exists and overwrite not specified\t'])
        del recorder

    def testFullOverwrite(self):
        # full on top of increment should fail without force
        GmtTimeFaker.setTime("1969-02-01")
        zfs = self.__mkBackupPool1Zfs(self.backupPool1Fs1Snapshots[0:3],
                                      self.backupPool1Fs2Snapshots[0:3])
        zfs.addSendRecvInfos(((("full", "srcPool1@zipper_1969-02-01T00:00:00_testBackupSet_full_backupPool1", "50000"), ("size", "50000")),
                              (("full", "srcPool1/srcPool1Fs2@zipper_1969-02-01T00:00:02_testBackupSet_full_backupPool1", "50000"), ("size", "50000"))))
        recorder = TestBackupRecorder(self.id())
        self.__twoFsBackup(zfs, recorder, BackupType.full, allowOverwrite=True)
        self.__assertActions(zfs,
                             ['zfs snapshot srcPool1@zipper_1969-02-01T00:00:00_testBackupSet_full_backupPool1',
                              'zfs send -P srcPool1@zipper_1969-02-01T00:00:00_testBackupSet_full_backupPool1 | zfs receive -F backupPool1/srcPool1@zipper_1969-02-01T00:00:00_testBackupSet_full_backupPool1',
                              'zfs snapshot srcPool1/srcPool1Fs2@zipper_1969-02-01T00:00:02_testBackupSet_full_backupPool1',
                              'zfs send -P srcPool1/srcPool1Fs2@zipper_1969-02-01T00:00:02_testBackupSet_full_backupPool1 | zfs receive -F backupPool1/srcPool1/srcPool1Fs2@zipper_1969-02-01T00:00:02_testBackupSet_full_backupPool1'])
        self.__assertRecorded(recorder,
                              ['1969-02-01T00:00:01\tfull\tsrcPool1@zipper_1969-02-01T00:00:00_testBackupSet_full_backupPool1\t\tbackupPool1/srcPool1@zipper_1969-02-01T00:00:00_testBackupSet_full_backupPool1\t50000\t\t',
                               '1969-02-01T00:00:03\tfull\tsrcPool1/srcPool1Fs2@zipper_1969-02-01T00:00:02_testBackupSet_full_backupPool1\t\tbackupPool1/srcPool1/srcPool1Fs2@zipper_1969-02-01T00:00:02_testBackupSet_full_backupPool1\t50000\t\t'])
        del recorder

    def testIncr1Pool2(self):
        # 1st incr on second pool, with different snapshots, should force a full which
        # is specific to this pool
        GmtTimeFaker.setTime("1932-02-01")
        zfs = self.__mkBackupPool2Zfs(self.backupPool1Fs1Snapshots[0:1],
                                      self.backupPool1Fs2Snapshots[0:1],
                                      (), (),
                                      includeBackupFileSystems=False)
        zfs.addSendRecvInfos(((("full", "srcPool1@zipper__testBackupSet_full_backupPool2", "50000"), ("size", "50000")),
                              (("full", "srcPool1/srcPool1Fs2@zipper__testBackupSet_full_backupPool2", "50000"), ("size", "50000"))))
        recorder = TestBackupRecorder(self.id())
        self.__twoFsBackup(zfs, recorder, BackupType.incr, backupPool=self.backupPool2)
        self.__assertActions(zfs,
                             ['zfs snapshot srcPool1@zipper_1932-02-01T00:00:00_testBackupSet_full_backupPool2',
                              'zfs send -P srcPool1@zipper_1932-02-01T00:00:00_testBackupSet_full_backupPool2 | zfs receive backupPool2/srcPool1@zipper_1932-02-01T00:00:00_testBackupSet_full_backupPool2',
                              'zfs snapshot srcPool1/srcPool1Fs2@zipper_1932-02-01T00:00:02_testBackupSet_full_backupPool2',
                              'zfs send -P srcPool1/srcPool1Fs2@zipper_1932-02-01T00:00:02_testBackupSet_full_backupPool2 | zfs receive backupPool2/srcPool1/srcPool1Fs2@zipper_1932-02-01T00:00:02_testBackupSet_full_backupPool2'])
        self.__assertRecorded(recorder, ['1932-02-01T00:00:01\tfull\tsrcPool1@zipper_1932-02-01T00:00:00_testBackupSet_full_backupPool2\t\tbackupPool2/srcPool1@zipper_1932-02-01T00:00:00_testBackupSet_full_backupPool2\t50000\t\t',
                                         '1932-02-01T00:00:03\tfull\tsrcPool1/srcPool1Fs2@zipper_1932-02-01T00:00:02_testBackupSet_full_backupPool2\t\tbackupPool2/srcPool1/srcPool1Fs2@zipper_1932-02-01T00:00:02_testBackupSet_full_backupPool2\t50000\t\t'])
        del recorder

    def testIncr3Pool2(self):
        # incr on second pool, which only has a full, should bring up-to-date add two
        # incremental.  Source has backups for both pools
        GmtTimeFaker.setTime("2022-02-01")
        zfs = self.__mkBackupPool2Zfs(self.backupPool2Fs1Snapshots[0:1]+self.backupPool1Fs1Snapshots[0:3],
                                      self.backupPool2Fs2Snapshots[0:1]+self.backupPool1Fs2Snapshots[0:3],
                                      self.backupPool2Fs1Snapshots[0:1],
                                      self.backupPool2Fs2Snapshots[0:1])
        zfs.addSendRecvInfos(((("incremental", "zipper__testBackupSet_full_backupPool2", "zipper_2015-02-01T17:30:34_testBackupSet_incr", "50000"), ("size", "50000")),
                              (("incremental", "zipper__testBackupSet_incr", "zipper__testBackupSet_incr", "50000"), ("size", "50000")),
                              (("incremental", "zipper__testBackupSet_incr", "srcPool1@zipper__testBackupSet_incr", "50000"), ("size", "50000")),
                              (("incremental", "zipper__testBackupSet_full_backupPool2", "zipper__testBackupSet_incr", "50000"), ("size", "50000")),
                              (("incremental", "zipper__testBackupSet_incr", "zipper__testBackupSet_incr", "50000"), ("size", "50000")),
                              (("incremental", "zipper__testBackupSet_incr", "srcPool1/srcPool1Fs2@zipper__testBackupSet_incr", "50000"), ("size", "50000"))))
        recorder = TestBackupRecorder(self.id())
        self.__twoFsBackup(zfs, recorder, BackupType.incr, backupPool=self.backupPool2)
        self.__assertActions(zfs,
                             ['zfs send -P -i zipper_2015-01-01T17:30:34_testBackupSet_full_backupPool2 zipper_2015-02-01T17:30:34_testBackupSet_incr | zfs receive backupPool2/srcPool1@zipper_2015-02-01T17:30:34_testBackupSet_incr',
                              'zfs send -P -i zipper_2015-02-01T17:30:34_testBackupSet_incr zipper_2015-03-02T17:30:34_testBackupSet_incr | zfs receive backupPool2/srcPool1@zipper_2015-03-02T17:30:34_testBackupSet_incr',
                              'zfs snapshot srcPool1@zipper_2022-02-01T00:00:02_testBackupSet_incr',
                              'zfs send -P -i zipper_2015-03-02T17:30:34_testBackupSet_incr srcPool1@zipper_2022-02-01T00:00:02_testBackupSet_incr | zfs receive backupPool2/srcPool1@zipper_2022-02-01T00:00:02_testBackupSet_incr',
                              'zfs send -P -i zipper_2015-01-01T17:30:34_testBackupSet_full_backupPool2 zipper_2015-02-01T17:30:34_testBackupSet_incr | zfs receive backupPool2/srcPool1/srcPool1Fs2@zipper_2015-02-01T17:30:34_testBackupSet_incr',
                              'zfs send -P -i zipper_2015-02-01T17:30:34_testBackupSet_incr zipper_2015-03-02T17:30:34_testBackupSet_incr | zfs receive backupPool2/srcPool1/srcPool1Fs2@zipper_2015-03-02T17:30:34_testBackupSet_incr',
                              'zfs snapshot srcPool1/srcPool1Fs2@zipper_2022-02-01T00:00:06_testBackupSet_incr',
                              'zfs send -P -i zipper_2015-03-02T17:30:34_testBackupSet_incr srcPool1/srcPool1Fs2@zipper_2022-02-01T00:00:06_testBackupSet_incr | zfs receive backupPool2/srcPool1/srcPool1Fs2@zipper_2022-02-01T00:00:06_testBackupSet_incr'])
        self.__assertRecorded(recorder,
                              ['2022-02-01T00:00:00\tincr\tzipper_2015-01-01T17:30:34_testBackupSet_full_backupPool2\tzipper_2015-02-01T17:30:34_testBackupSet_incr\tbackupPool2/srcPool1@zipper_2015-02-01T17:30:34_testBackupSet_incr\tsrcPool1/srcPool1Fs2@zipper__testBackupSet_incr\t\t',
                               '2022-02-01T00:00:01\tincr\tzipper_2015-02-01T17:30:34_testBackupSet_incr\tzipper_2015-03-02T17:30:34_testBackupSet_incr\tbackupPool2/srcPool1@zipper_2015-03-02T17:30:34_testBackupSet_incr\tzipper__testBackupSet_incr\t\t',
                               '2022-02-01T00:00:03\tincr\tzipper_2015-03-02T17:30:34_testBackupSet_incr\tsrcPool1@zipper_2022-02-01T00:00:02_testBackupSet_incr\tbackupPool2/srcPool1@zipper_2022-02-01T00:00:02_testBackupSet_incr\tzipper__testBackupSet_incr\t\t',
                               '2022-02-01T00:00:04\tincr\tzipper_2015-01-01T17:30:34_testBackupSet_full_backupPool2\tzipper_2015-02-01T17:30:34_testBackupSet_incr\tbackupPool2/srcPool1/srcPool1Fs2@zipper_2015-02-01T17:30:34_testBackupSet_incr\tsrcPool1@zipper__testBackupSet_incr\t\t',
                               '2022-02-01T00:00:05\tincr\tzipper_2015-02-01T17:30:34_testBackupSet_incr\tzipper_2015-03-02T17:30:34_testBackupSet_incr\tbackupPool2/srcPool1/srcPool1Fs2@zipper_2015-03-02T17:30:34_testBackupSet_incr\tzipper__testBackupSet_incr\t\t',
                               '2022-02-01T00:00:07\tincr\tzipper_2015-03-02T17:30:34_testBackupSet_incr\tsrcPool1/srcPool1Fs2@zipper_2022-02-01T00:00:06_testBackupSet_incr\tbackupPool2/srcPool1/srcPool1Fs2@zipper_2022-02-01T00:00:06_testBackupSet_incr\tzipper_2015-02-01T17:30:34_testBackupSet_incr\t\t'])
        del recorder

    def testIncrFailPool2(self):
        #fail on existing file systems, then redo it allowOverwrite
        GmtTimeFaker.setTime("1977-02-01")
        zfs = self.__mkBackupPool2Zfs(self.backupPool1Fs1Snapshots[0:1],
                                      self.backupPool1Fs2Snapshots[0:1],
                                      (), ())
        zfs.addSendRecvInfos(((("full", "srcPool1@zipper__testBackupSet_full_backupPool2", "50000"), ("size", "50000")),
                              (("full", "srcPool1/srcPool1Fs2@zipper__testBackupSet_full_backupPool2", "50000"), ("size", "50000"))))
        recorder = TestBackupRecorder(self.id())

        with self.assertRaisesRegexp(BackupError, "^incremental backup of srcPool1 to backupPool2/srcPool1: no common full backup snapshot, backup pool backupPool2 already has the file system, must specify allowOverwrite to create a new full backup$"):
            self.__twoFsBackup(zfs, recorder, BackupType.incr, backupPool=self.backupPool2)

        self.__twoFsBackup(zfs, recorder, BackupType.incr, backupPool=self.backupPool2, allowOverwrite=True)
        self.__assertActions(zfs,
                             ['zfs snapshot srcPool1@zipper_1977-02-01T00:00:01_testBackupSet_full_backupPool2',
                              'zfs send -P srcPool1@zipper_1977-02-01T00:00:01_testBackupSet_full_backupPool2 | zfs receive -F backupPool2/srcPool1@zipper_1977-02-01T00:00:01_testBackupSet_full_backupPool2',
                              'zfs snapshot srcPool1/srcPool1Fs2@zipper_1977-02-01T00:00:03_testBackupSet_full_backupPool2',
                              'zfs send -P srcPool1/srcPool1Fs2@zipper_1977-02-01T00:00:03_testBackupSet_full_backupPool2 | zfs receive -F backupPool2/srcPool1/srcPool1Fs2@zipper_1977-02-01T00:00:03_testBackupSet_full_backupPool2'])
        self.__assertRecorded(recorder,
                            ['1977-02-01T00:00:00\terror\tsrcPool1\t\t\tBackupError\tincremental backup of srcPool1 to backupPool2/srcPool1: no common full backup snapshot, backup pool backupPool2 already has the file system, must specify allowOverwrite to create a new full backup\t',
                             '1977-02-01T00:00:02\tfull\tsrcPool1@zipper_1977-02-01T00:00:01_testBackupSet_full_backupPool2\t\tbackupPool2/srcPool1@zipper_1977-02-01T00:00:01_testBackupSet_full_backupPool2\t50000\t\t',
                             '1977-02-01T00:00:04\tfull\tsrcPool1/srcPool1Fs2@zipper_1977-02-01T00:00:03_testBackupSet_full_backupPool2\t\tbackupPool2/srcPool1/srcPool1Fs2@zipper_1977-02-01T00:00:03_testBackupSet_full_backupPool2\t50000\t\t'])
        del recorder

    def testBackupSetToPool1Full(self):
        GmtTimeFaker.setTime("1982-02-01")
        zfsConf = ((self.srcPool1, ((self.srcPool1Fs1, ()),
                                    (self.srcPool1Fs2, ()))),
                   ((self.backupPool1, ())),
                   (self.backupPool2Off, ()))
        zfs = ZfsMock(zfsConf)
        zfs.addSendRecvInfos(((("full", "srcPool1@zipper__testBackupSet_full_backupPool1", "50000"), ("size", "50000")),
                              (("full", "srcPool1/srcPool1Fs2@zipper__testBackupSet_full_backupPool1", "50000"), ("size", "50000"))))
        recorder = TestBackupRecorder(self.id())
        bsb = BackupSetBackup(zfs, recorder, self.backupConf1, allowOverwrite=False)
        bsb.backupAll(BackupType.full)
        self.__assertActions(zfs,['zfs snapshot srcPool1@zipper_1982-02-01T00:00:00_testBackupSet_full_backupPool1',
                                  'zfs send -P srcPool1@zipper_1982-02-01T00:00:00_testBackupSet_full_backupPool1 | zfs receive backupPool1/srcPool1@zipper_1982-02-01T00:00:00_testBackupSet_full_backupPool1',
                                  'zfs snapshot srcPool1/srcPool1Fs2@zipper_1982-02-01T00:00:02_testBackupSet_full_backupPool1',
                                  'zfs send -P srcPool1/srcPool1Fs2@zipper_1982-02-01T00:00:02_testBackupSet_full_backupPool1 | zfs receive backupPool1/srcPool1/srcPool1Fs2@zipper_1982-02-01T00:00:02_testBackupSet_full_backupPool1'])
        self.__assertRecorded(recorder,
                              ['1982-02-01T00:00:01\tfull\tsrcPool1@zipper_1982-02-01T00:00:00_testBackupSet_full_backupPool1\t\tbackupPool1/srcPool1@zipper_1982-02-01T00:00:00_testBackupSet_full_backupPool1\t50000\t\t',
                               '1982-02-01T00:00:03\tfull\tsrcPool1/srcPool1Fs2@zipper_1982-02-01T00:00:02_testBackupSet_full_backupPool1\t\tbackupPool1/srcPool1/srcPool1Fs2@zipper_1982-02-01T00:00:02_testBackupSet_full_backupPool1\t50000\t\t'])
        del recorder

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(BackupSnapshotTests))
    suite.addTest(unittest.makeSuite(BackuperTests))
    return suite

if __name__ == '__main__':
    unittest.main()
