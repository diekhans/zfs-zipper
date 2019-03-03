"""
Test of zfs-zipper tests of library functions.
"""

import os
import sys
import unittest
import tempfile
sys.path.insert(0, "../lib/zfs-zipper")
from zfszipper import typeOps
from zfszipper import loggingOps
from zfszipper.backup import BackupSnapshot, FsBackup, BackupError, BackupSetBackup, BackupRecorder
from zfszipper.zfs import ZfsPool, ZfsSnapshot, ZfsPoolHealth
from zfszipper.config import BackupPoolConf, BackupSetConf, SourceFileSystemConf
from zfsMock import ZfsMock, fakeZfsFileSystem
from zfszipper.typeOps import splitLinesToRows
import logging
logging.basicConfig(filename="/dev/null")

# Notes:
#   - don't remember why the recorder is explicitly deleted

# enable this to get more information when debugging
if False:
    loggingOps.setupStderrLogger(logging.DEBUG)


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
        typeOps.currentGmtTimeStrFunc = timer
        return timer

class TestBackupRecorder(BackupRecorder):
    "adds functionality for build on tests and cleaning up"

    def __init__(self, testId):
        fd, self.tmpTsv = tempfile.mkstemp(".tsv", "backup-test." + testId)
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
    testName = "zipper_1979-01-20T16:14:05_funbackset"

    # use old-style names with _full/_incr extensions to see if they work
    fullTestOldName = "zipper_1979-01-20T16:14:05_funbackset_full"
    incrTestOldName = "zipper_2001-01-20T16:14:05_funbackset_incr"

    @staticmethod
    def _mkFsSnapshot(fs, ss):
        return fs + "@" + ss

    def testParse(self):
        ss = BackupSnapshot.createFromSnapshotName(self.testName)
        self.assertEqual(self.testName, str(ss))

    def testFullParseOld(self):
        ss = BackupSnapshot.createFromSnapshotName(self.fullTestOldName)
        self.assertEqual(self.fullTestOldName, str(ss))

    def testIncrParseOld(self):
        ss = BackupSnapshot.createFromSnapshotName(self.incrTestOldName)
        self.assertEqual(self.incrTestOldName, str(ss))

    def testParseFs(self):
        fsSSName = self._mkFsSnapshot(self.testFs1, self.testName)
        ss = BackupSnapshot.createFromSnapshotName(fsSSName)
        self.assertEqual(fsSSName, str(ss))

    def testFullParseFsOld(self):
        fsSSName = self._mkFsSnapshot(self.testFs1, self.fullTestOldName)
        ss = BackupSnapshot.createFromSnapshotName(fsSSName)
        self.assertEqual(fsSSName, str(ss))

    def testIncrParseFs(self):
        fsSSName = self._mkFsSnapshot(self.testFs1, self.incrTestOldName)
        ss = BackupSnapshot.createFromSnapshotName(fsSSName)
        self.assertEqual(fsSSName, str(ss))

    def testDropFsParse(self):
        fsSSName = self._mkFsSnapshot(self.testFs1, self.testName)
        ss = BackupSnapshot.createFromSnapshotName(fsSSName, dropFileSystem=True)
        self.assertEqual(self.testName, str(ss))

    def testCurrent(self):
        ss = BackupSnapshot.createCurrent("someset", fakeZfsFileSystem("somepool/somefs"))
        self.assertRegex(str(ss), "^somepool/somefs@zipper_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}_someset$")

    def testFromSnapshot(self):
        ss = BackupSnapshot.createFromSnapshotName(self.testName)
        ss2 = BackupSnapshot.createFromSnapshot(ss)
        self.assertEqual(self.testName, str(ss2))

    def testFromSnapshotSubstFS(self):
        fs1SSName = self._mkFsSnapshot(self.testFs1, self.testName)
        fs2SSName = self._mkFsSnapshot(self.testFs2, self.testName)
        ss = BackupSnapshot.createFromSnapshotName(fs1SSName)
        ss2 = BackupSnapshot.createFromSnapshot(ss, fileSystem=fakeZfsFileSystem(self.testFs2))
        self.assertEqual(fs2SSName, str(ss2))

class BackuperTests(unittest.TestCase):

    @staticmethod
    def _mkSnapshots(pool, fs, snapshotNames):
        return tuple([ZfsSnapshot.factory(fs.name, ss)
                      for ss in snapshotNames])

    backupConf1 = BackupSetConf("testBackupSet",
                                [SourceFileSystemConf("srcPool1/srcPool1Fs1"),
                                 "srcPool1/srcPool1Fs2"],
                                [BackupPoolConf("backupPool1"),
                                 BackupPoolConf("backupPool2")])
    srcPool1 = ZfsPool("srcPool1", ZfsPoolHealth.ONLINE)
    srcPool1Fs1 = fakeZfsFileSystem("srcPool1/srcPool1Fs1")
    srcPool1Fs1StraySnapNames = ("otherSnap1", "otherSnap2")
    srcPool1Fs2 = fakeZfsFileSystem("srcPool1/srcPool1Fs2")

    backupPool1 = ZfsPool("backupPool1", ZfsPoolHealth.ONLINE)
    backupPool1Fs1 = fakeZfsFileSystem("backupPool1/srcPool1/srcPool1Fs1")
    backupPool1Fs2 = fakeZfsFileSystem("backupPool1/srcPool1/srcPool1Fs2")

    backupPool2 = ZfsPool("backupPool2", ZfsPoolHealth.ONLINE)
    backupPool2Fs1 = fakeZfsFileSystem("backupPool2/srcPool1/srcPool1Fs1")
    backupPool2Fs2 = fakeZfsFileSystem("backupPool2/srcPool1/srcPool1Fs2")

    backupPool2Off = ZfsPool("backupPool2", ZfsPoolHealth.OFFLINE)

    pool1Fs1SnapNames = ('zipper_1932-01-01T17:30:34_testBackupSet',
                         'zipper_1932-02-01T17:30:34_testBackupSet',
                         'zipper_1932-03-02T17:30:34_testBackupSet')
    pool1Fs2SnapNames = ('zipper_1932-01-01T17:30:34_testBackupSet',
                         'zipper_1932-02-01T17:30:34_testBackupSet',
                         'zipper_1932-03-02T17:30:34_testBackupSet')
    pool2Fs1SnapNames = ('zipper_1932-01-01T17:30:34_testBackupSet',
                         'zipper_1932-02-01T17:30:34_testBackupSet',
                         'zipper_1932-03-02T17:30:34_testBackupSet')
    pool2Fs2SnapNames = ('zipper_1932-01-01T17:30:34_testBackupSet',
                         'zipper_1932-02-01T17:30:34_testBackupSet',
                         'zipper_1932-03-02T17:30:34_testBackupSet')

    def _mkInitialZfs(self):
        zfs = ZfsMock()
        zfs.add(self.srcPool1, self.srcPool1Fs1, self.srcPool1Fs1StraySnapNames)
        zfs.add(self.srcPool1, self.srcPool1Fs2)
        zfs.add(self.backupPool1)
        return zfs

    def _mkBackupPool1Zfs(self, sourceFs1SnapNames=(), sourceFs2SnapNames=(),
                          backupFs1SnapNames=(), backupFs2SnapNames=()):
        """zfs configured for backupPool1 cases.  The backup file systems are only
        created if snapshots are specified.  """
        zfs = ZfsMock()
        zfs.add(self.srcPool1, self.srcPool1Fs1, self.srcPool1Fs1StraySnapNames + sourceFs1SnapNames)
        zfs.add(self.srcPool1, self.srcPool1Fs2, sourceFs2SnapNames)
        zfs.add(self.backupPool1)
        if backupFs1SnapNames:
            zfs.add(self.backupPool1, self.backupPool1Fs1, backupFs1SnapNames)
        if backupFs2SnapNames:
            zfs.add(self.backupPool1, self.backupPool1Fs2, backupFs2SnapNames)
        return zfs

    def _mkBackupPool2Zfs(self, sourceFs1SnapNames=(), sourceFs2SnapNames=(),
                          backupFs1SnapNames=(), backupFs2SnapNames=()):
        """zfs configured for backupPool2 cases.  The backup file systems are only
        created if snapshots are specified"""
        zfs = ZfsMock()
        zfs.add(self.srcPool1, self.srcPool1Fs1, self.srcPool1Fs1StraySnapNames + sourceFs1SnapNames)
        zfs.add(self.srcPool1, self.srcPool1Fs2, sourceFs2SnapNames)
        zfs.add(self.backupPool2)
        if backupFs1SnapNames:
            zfs.add(self.backupPool2, self.backupPool2Fs1, backupFs1SnapNames)
        if backupFs2SnapNames:
            zfs.add(self.backupPool2, self.backupPool2Fs2, backupFs2SnapNames)
        return zfs

    def _setupFsBackup1(self, zfs, recorder, sourceFileSystemName, backupPool=backupPool1, allowOverwrite=False, *, forceOverwrite=False):
        return FsBackup(zfs, recorder, self.backupConf1,
                        zfs.getFileSystem(sourceFileSystemName),
                        backupPool, allowOverwrite,
                        forceOverwrite=forceOverwrite)

    def _twoFsBackup(self, zfs, recorder, backupPool=backupPool1, allowOverwrite=False, *, forceOverwrite=False):
        fsBackup = self._setupFsBackup1(zfs, recorder, "srcPool1/srcPool1Fs1", backupPool=backupPool, allowOverwrite=allowOverwrite, forceOverwrite=forceOverwrite)
        fsBackup.backup()
        fsBackup = self._setupFsBackup1(zfs, recorder, "srcPool1/srcPool1Fs2", backupPool=backupPool, allowOverwrite=allowOverwrite, forceOverwrite=forceOverwrite)
        fsBackup.backup()

    def _assertActions(self, zfs, expected):
        self.maxDiff = None
        try:
            self.assertEqual(expected, zfs.actions)
        except Exception:
            print("Error: actions not what is expected for", self.id(), file=sys.stderr)
            for l in expected:
                print("   <'" + l + "',", file=sys.stderr)
            for l in zfs.actions:
                print("   >'" + l + "',", file=sys.stderr)
            raise

    def _assertRecorded(self, recorder, expected):
        "expected should not include header line"
        self.maxDiff = None
        header = 'time	backupSet	backupPool	action	src1Snap	src2Snap	backupSnap	size	exception	info'
        expected = [header] + list(expected)
        got = recorder.readLines()
        try:
            self.assertEqual(expected, got)
        except Exception:
            print("Error: recorded not what is expected for ", self.id(), file=sys.stderr)
            for l in expected:
                print("   <'" + l + "',", file=sys.stderr)
            for l in got:
                print("   >'" + l + "',", file=sys.stderr)
            raise

    def testInitialFull(self):
        GmtTimeFaker.setTime("2001-01-01")
        zfs = self._mkInitialZfs()
        recorder = TestBackupRecorder(self.id())
        self._twoFsBackup(zfs, recorder)

        self._assertActions(zfs,
                            ['zfs snapshot srcPool1/srcPool1Fs1@zipper_2001-01-01T00:00:00_testBackupSet',
                             'zfs send -P srcPool1/srcPool1Fs1@zipper_2001-01-01T00:00:00_testBackupSet | zfs receive backupPool1/srcPool1/srcPool1Fs1@zipper_2001-01-01T00:00:00_testBackupSet',
                             'zfs snapshot srcPool1/srcPool1Fs2@zipper_2001-01-01T00:00:02_testBackupSet',
                             'zfs send -P srcPool1/srcPool1Fs2@zipper_2001-01-01T00:00:02_testBackupSet | zfs receive backupPool1/srcPool1/srcPool1Fs2@zipper_2001-01-01T00:00:02_testBackupSet'])
        self._assertRecorded(recorder,
                             ['2001-01-01T00:00:01	testBackupSet	backupPool1	full	srcPool1/srcPool1Fs1@zipper_2001-01-01T00:00:00_testBackupSet		backupPool1/srcPool1/srcPool1Fs1@zipper_2001-01-01T00:00:00_testBackupSet	50000		',
                              '2001-01-01T00:00:03	testBackupSet	backupPool1	full	srcPool1/srcPool1Fs2@zipper_2001-01-01T00:00:02_testBackupSet		backupPool1/srcPool1/srcPool1Fs2@zipper_2001-01-01T00:00:02_testBackupSet	50000		'])
        del recorder

    def testIncr1(self):
        # 1st incr
        GmtTimeFaker.setTime("2001-01-02")
        zfs = self._mkBackupPool1Zfs(self.pool1Fs1SnapNames[0:1], self.pool1Fs2SnapNames[0:1])
        recorder = TestBackupRecorder(self.id())
        self._twoFsBackup(zfs, recorder)
        self._assertActions(zfs,
                            ['zfs send -P srcPool1/srcPool1Fs1@zipper_1932-01-01T17:30:34_testBackupSet | zfs receive backupPool1/srcPool1/srcPool1Fs1@zipper_1932-01-01T17:30:34_testBackupSet',
                             'zfs snapshot srcPool1/srcPool1Fs1@zipper_2001-01-02T00:00:01_testBackupSet',
                             'zfs send -P -i srcPool1/srcPool1Fs1@zipper_1932-01-01T17:30:34_testBackupSet srcPool1/srcPool1Fs1@zipper_2001-01-02T00:00:01_testBackupSet | zfs receive backupPool1/srcPool1/srcPool1Fs1@zipper_2001-01-02T00:00:01_testBackupSet',
                             'zfs send -P srcPool1/srcPool1Fs2@zipper_1932-01-01T17:30:34_testBackupSet | zfs receive backupPool1/srcPool1/srcPool1Fs2@zipper_1932-01-01T17:30:34_testBackupSet',
                             'zfs snapshot srcPool1/srcPool1Fs2@zipper_2001-01-02T00:00:04_testBackupSet',
                             'zfs send -P -i srcPool1/srcPool1Fs2@zipper_1932-01-01T17:30:34_testBackupSet srcPool1/srcPool1Fs2@zipper_2001-01-02T00:00:04_testBackupSet | zfs receive backupPool1/srcPool1/srcPool1Fs2@zipper_2001-01-02T00:00:04_testBackupSet'])
        self._assertRecorded(recorder,
                             ['2001-01-02T00:00:00	testBackupSet	backupPool1	full	srcPool1/srcPool1Fs1@zipper_1932-01-01T17:30:34_testBackupSet		backupPool1/srcPool1/srcPool1Fs1@zipper_1932-01-01T17:30:34_testBackupSet	50000		',
                              '2001-01-02T00:00:02	testBackupSet	backupPool1	incr	srcPool1/srcPool1Fs1@zipper_1932-01-01T17:30:34_testBackupSet	srcPool1/srcPool1Fs1@zipper_2001-01-02T00:00:01_testBackupSet	backupPool1/srcPool1/srcPool1Fs1@zipper_2001-01-02T00:00:01_testBackupSet	50000		',
                              '2001-01-02T00:00:03	testBackupSet	backupPool1	full	srcPool1/srcPool1Fs2@zipper_1932-01-01T17:30:34_testBackupSet		backupPool1/srcPool1/srcPool1Fs2@zipper_1932-01-01T17:30:34_testBackupSet	50000		',
                              '2001-01-02T00:00:05	testBackupSet	backupPool1	incr	srcPool1/srcPool1Fs2@zipper_1932-01-01T17:30:34_testBackupSet	srcPool1/srcPool1Fs2@zipper_2001-01-02T00:00:04_testBackupSet	backupPool1/srcPool1/srcPool1Fs2@zipper_2001-01-02T00:00:04_testBackupSet	50000		'])
        del recorder

    def testIncr2(self):
        # 2nd incr
        GmtTimeFaker.setTime("1999-02-01")
        zfs = self._mkBackupPool1Zfs(self.pool1Fs1SnapNames[0:2], self.pool1Fs2SnapNames[0:2],
                                     self.pool1Fs1SnapNames[0:2], self.pool1Fs2SnapNames[0:2])
        recorder = TestBackupRecorder(self.id())
        self._twoFsBackup(zfs, recorder)
        self._assertActions(zfs,
                            ['zfs snapshot srcPool1/srcPool1Fs1@zipper_1999-02-01T00:00:00_testBackupSet',
                             'zfs send -P -i srcPool1/srcPool1Fs1@zipper_1932-02-01T17:30:34_testBackupSet srcPool1/srcPool1Fs1@zipper_1999-02-01T00:00:00_testBackupSet | zfs receive backupPool1/srcPool1/srcPool1Fs1@zipper_1999-02-01T00:00:00_testBackupSet',
                             'zfs snapshot srcPool1/srcPool1Fs2@zipper_1999-02-01T00:00:02_testBackupSet',
                             'zfs send -P -i srcPool1/srcPool1Fs2@zipper_1932-02-01T17:30:34_testBackupSet srcPool1/srcPool1Fs2@zipper_1999-02-01T00:00:02_testBackupSet | zfs receive backupPool1/srcPool1/srcPool1Fs2@zipper_1999-02-01T00:00:02_testBackupSet'])
        self._assertRecorded(recorder,
                             ['1999-02-01T00:00:01	testBackupSet	backupPool1	incr	srcPool1/srcPool1Fs1@zipper_1932-02-01T17:30:34_testBackupSet	srcPool1/srcPool1Fs1@zipper_1999-02-01T00:00:00_testBackupSet	backupPool1/srcPool1/srcPool1Fs1@zipper_1999-02-01T00:00:00_testBackupSet	50000		',
                              '1999-02-01T00:00:03	testBackupSet	backupPool1	incr	srcPool1/srcPool1Fs2@zipper_1932-02-01T17:30:34_testBackupSet	srcPool1/srcPool1Fs2@zipper_1999-02-01T00:00:02_testBackupSet	backupPool1/srcPool1/srcPool1Fs2@zipper_1999-02-01T00:00:02_testBackupSet	50000		'])
        del recorder

    def FIXME_testFullOverwrite(self):
        # full on top of increment should fail without force
        GmtTimeFaker.setTime("1969-02-01")
        zfs = self._mkBackupPool1Zfs(self.pool1Fs1SnapNames[0:3], self.pool1Fs2SnapNames[0:3],
                                     self.pool1Fs1SnapNames[0:3], self.pool1Fs2SnapNames[0:3])
        recorder = TestBackupRecorder(self.id())
        self._twoFsBackup(zfs, recorder, allowOverwrite=True, forceOverwrite=True)
        self._assertActions(zfs,
                            ['zfs snapshot srcPool1/srcPool1Fs1@zipper_1969-02-01T00:00:00_testBackupSet',
                             'zfs send -P srcPool1/srcPool1Fs1@zipper_1969-02-01T00:00:00_testBackupSet | zfs receive -F backupPool1/srcPool1/srcPool1Fs1@zipper_1969-02-01T00:00:00_testBackupSet',
                             'zfs snapshot srcPool1/srcPool1Fs2@zipper_1969-02-01T00:00:02_testBackupSet',
                             'zfs send -P srcPool1/srcPool1Fs2@zipper_1969-02-01T00:00:02_testBackupSet | zfs receive -F backupPool1/srcPool1/srcPool1Fs2@zipper_1969-02-01T00:00:02_testBackupSet'])
        self._assertRecorded(recorder,
                             ['1969-02-01T00:00:01	testBackupSet	backupPool1	full	srcPool1/srcPool1Fs1@zipper_1969-02-01T00:00:00_testBackupSet		backupPool1/srcPool1/srcPool1Fs1@zipper_1969-02-01T00:00:00_testBackupSet	50000		',
                              '1969-02-01T00:00:03	testBackupSet	backupPool1	full	srcPool1/srcPool1Fs2@zipper_1969-02-01T00:00:02_testBackupSet		backupPool1/srcPool1/srcPool1Fs2@zipper_1969-02-01T00:00:02_testBackupSet	50000		'])
        del recorder

    def testIncr1Pool2(self):
        # 1st incr on second pool, with different snapshots, should force a full which
        # is specific to this pool
        GmtTimeFaker.setTime("1932-02-01")
        zfs = self._mkBackupPool2Zfs(self.pool1Fs1SnapNames[0:1], self.pool1Fs2SnapNames[0:1])
        zfs.add(self.backupPool2)
        recorder = TestBackupRecorder(self.id())
        self._twoFsBackup(zfs, recorder, backupPool=self.backupPool2)
        self._assertActions(zfs,
                            ['zfs send -P srcPool1/srcPool1Fs1@zipper_1932-01-01T17:30:34_testBackupSet | zfs receive backupPool2/srcPool1/srcPool1Fs1@zipper_1932-01-01T17:30:34_testBackupSet',
                             'zfs snapshot srcPool1/srcPool1Fs1@zipper_1932-02-01T00:00:01_testBackupSet',
                             'zfs send -P -i srcPool1/srcPool1Fs1@zipper_1932-01-01T17:30:34_testBackupSet srcPool1/srcPool1Fs1@zipper_1932-02-01T00:00:01_testBackupSet | zfs receive backupPool2/srcPool1/srcPool1Fs1@zipper_1932-02-01T00:00:01_testBackupSet',
                             'zfs send -P srcPool1/srcPool1Fs2@zipper_1932-01-01T17:30:34_testBackupSet | zfs receive backupPool2/srcPool1/srcPool1Fs2@zipper_1932-01-01T17:30:34_testBackupSet',
                             'zfs snapshot srcPool1/srcPool1Fs2@zipper_1932-02-01T00:00:04_testBackupSet',
                             'zfs send -P -i srcPool1/srcPool1Fs2@zipper_1932-01-01T17:30:34_testBackupSet srcPool1/srcPool1Fs2@zipper_1932-02-01T00:00:04_testBackupSet | zfs receive backupPool2/srcPool1/srcPool1Fs2@zipper_1932-02-01T00:00:04_testBackupSet'])
        self._assertRecorded(recorder, ['1932-02-01T00:00:00	testBackupSet	backupPool2	full	srcPool1/srcPool1Fs1@zipper_1932-01-01T17:30:34_testBackupSet		backupPool2/srcPool1/srcPool1Fs1@zipper_1932-01-01T17:30:34_testBackupSet	50000		',
                                        '1932-02-01T00:00:02	testBackupSet	backupPool2	incr	srcPool1/srcPool1Fs1@zipper_1932-01-01T17:30:34_testBackupSet	srcPool1/srcPool1Fs1@zipper_1932-02-01T00:00:01_testBackupSet	backupPool2/srcPool1/srcPool1Fs1@zipper_1932-02-01T00:00:01_testBackupSet	50000		',
                                        '1932-02-01T00:00:03	testBackupSet	backupPool2	full	srcPool1/srcPool1Fs2@zipper_1932-01-01T17:30:34_testBackupSet		backupPool2/srcPool1/srcPool1Fs2@zipper_1932-01-01T17:30:34_testBackupSet	50000		',
                                        '1932-02-01T00:00:05	testBackupSet	backupPool2	incr	srcPool1/srcPool1Fs2@zipper_1932-01-01T17:30:34_testBackupSet	srcPool1/srcPool1Fs2@zipper_1932-02-01T00:00:04_testBackupSet	backupPool2/srcPool1/srcPool1Fs2@zipper_1932-02-01T00:00:04_testBackupSet	50000		'])
        del recorder

    def testIncr3Pool2(self):
        # incr on second pool, which only has a full, should bring up-to-date add two
        # incremental.  Source has backups for both pools
        GmtTimeFaker.setTime("2022-02-01")
        zfs = self._mkBackupPool2Zfs(self.pool2Fs1SnapNames[0:3], self.pool2Fs2SnapNames[0:3],
                                     self.pool2Fs1SnapNames[0:1], self.pool2Fs2SnapNames[0:1])
        recorder = TestBackupRecorder(self.id())
        self._twoFsBackup(zfs, recorder, backupPool=self.backupPool2)
        self._assertActions(zfs,
                            ['zfs send -P -i srcPool1/srcPool1Fs1@zipper_1932-01-01T17:30:34_testBackupSet srcPool1/srcPool1Fs1@zipper_1932-02-01T17:30:34_testBackupSet | zfs receive backupPool2/srcPool1/srcPool1Fs1@zipper_1932-02-01T17:30:34_testBackupSet',
                             'zfs send -P -i srcPool1/srcPool1Fs1@zipper_1932-02-01T17:30:34_testBackupSet srcPool1/srcPool1Fs1@zipper_1932-03-02T17:30:34_testBackupSet | zfs receive backupPool2/srcPool1/srcPool1Fs1@zipper_1932-03-02T17:30:34_testBackupSet',
                             'zfs snapshot srcPool1/srcPool1Fs1@zipper_2022-02-01T00:00:02_testBackupSet',
                             'zfs send -P -i srcPool1/srcPool1Fs1@zipper_1932-03-02T17:30:34_testBackupSet srcPool1/srcPool1Fs1@zipper_2022-02-01T00:00:02_testBackupSet | zfs receive backupPool2/srcPool1/srcPool1Fs1@zipper_2022-02-01T00:00:02_testBackupSet',
                             'zfs send -P -i srcPool1/srcPool1Fs2@zipper_1932-01-01T17:30:34_testBackupSet srcPool1/srcPool1Fs2@zipper_1932-02-01T17:30:34_testBackupSet | zfs receive backupPool2/srcPool1/srcPool1Fs2@zipper_1932-02-01T17:30:34_testBackupSet',
                             'zfs send -P -i srcPool1/srcPool1Fs2@zipper_1932-02-01T17:30:34_testBackupSet srcPool1/srcPool1Fs2@zipper_1932-03-02T17:30:34_testBackupSet | zfs receive backupPool2/srcPool1/srcPool1Fs2@zipper_1932-03-02T17:30:34_testBackupSet',
                             'zfs snapshot srcPool1/srcPool1Fs2@zipper_2022-02-01T00:00:06_testBackupSet',
                             'zfs send -P -i srcPool1/srcPool1Fs2@zipper_1932-03-02T17:30:34_testBackupSet srcPool1/srcPool1Fs2@zipper_2022-02-01T00:00:06_testBackupSet | zfs receive backupPool2/srcPool1/srcPool1Fs2@zipper_2022-02-01T00:00:06_testBackupSet'])
        self._assertRecorded(recorder,
                             ['2022-02-01T00:00:00	testBackupSet	backupPool2	incr	srcPool1/srcPool1Fs1@zipper_1932-01-01T17:30:34_testBackupSet	srcPool1/srcPool1Fs1@zipper_1932-02-01T17:30:34_testBackupSet	backupPool2/srcPool1/srcPool1Fs1@zipper_1932-02-01T17:30:34_testBackupSet	50000		',
                              '2022-02-01T00:00:01	testBackupSet	backupPool2	incr	srcPool1/srcPool1Fs1@zipper_1932-02-01T17:30:34_testBackupSet	srcPool1/srcPool1Fs1@zipper_1932-03-02T17:30:34_testBackupSet	backupPool2/srcPool1/srcPool1Fs1@zipper_1932-03-02T17:30:34_testBackupSet	50000		',
                              '2022-02-01T00:00:03	testBackupSet	backupPool2	incr	srcPool1/srcPool1Fs1@zipper_1932-03-02T17:30:34_testBackupSet	srcPool1/srcPool1Fs1@zipper_2022-02-01T00:00:02_testBackupSet	backupPool2/srcPool1/srcPool1Fs1@zipper_2022-02-01T00:00:02_testBackupSet	50000		',
                              '2022-02-01T00:00:04	testBackupSet	backupPool2	incr	srcPool1/srcPool1Fs2@zipper_1932-01-01T17:30:34_testBackupSet	srcPool1/srcPool1Fs2@zipper_1932-02-01T17:30:34_testBackupSet	backupPool2/srcPool1/srcPool1Fs2@zipper_1932-02-01T17:30:34_testBackupSet	50000		',
                              '2022-02-01T00:00:05	testBackupSet	backupPool2	incr	srcPool1/srcPool1Fs2@zipper_1932-02-01T17:30:34_testBackupSet	srcPool1/srcPool1Fs2@zipper_1932-03-02T17:30:34_testBackupSet	backupPool2/srcPool1/srcPool1Fs2@zipper_1932-03-02T17:30:34_testBackupSet	50000		',
                              '2022-02-01T00:00:07	testBackupSet	backupPool2	incr	srcPool1/srcPool1Fs2@zipper_1932-03-02T17:30:34_testBackupSet	srcPool1/srcPool1Fs2@zipper_2022-02-01T00:00:06_testBackupSet	backupPool2/srcPool1/srcPool1Fs2@zipper_2022-02-01T00:00:06_testBackupSet	50000		'])
        del recorder

    def FIXME_testIncrFailPool2(self):
        # fail on existing file systems, then redo it allowOverwrite
        GmtTimeFaker.setTime("1977-02-01")
        zfs = self._mkBackupPool2Zfs(self.pool1Fs1SnapNames[0:1], self.pool1Fs2SnapNames[0:1],
                                     (), ())
        recorder = TestBackupRecorder(self.id())

        with self.assertRaisesRegex(BackupError, "^backup of srcPool1 to backupPool2/srcPool1: no common full backup snapshot, yet backup pool backupPool2 already has the file system, must specify --allowOverwrite to create a new backup$"):
            self._twoFsBackup(zfs, recorder, backupPool=self.backupPool2)

        self._twoFsBackup(zfs, recorder, backupPool=self.backupPool2, allowOverwrite=True)
        self._assertActions(zfs,
                            ['zfs send -P srcPool1/srcPool1Fs1@zipper_1932-01-01T17:30:34_testBackupSet | zfs receive -F backupPool2/srcPool1/srcPool1Fs1@zipper_1932-01-01T17:30:34_testBackupSet',
                             'zfs snapshot srcPool1/srcPool1Fs1@zipper_1977-02-01T00:00:02_testBackupSet',
                             'zfs send -P -i srcPool1/srcPool1Fs1@zipper_1932-01-01T17:30:34_testBackupSet srcPool1/srcPool1Fs1@zipper_1977-02-01T00:00:02_testBackupSet | zfs receive backupPool2/srcPool1/srcPool1Fs1@zipper_1977-02-01T00:00:02_testBackupSet',
                             'zfs send -P srcPool1/srcPool1Fs1@zipper_1932-01-01T17:30:34_testBackupSet | zfs receive -F backupPool2/srcPool1/srcPool1Fs2@zipper_1932-01-01T17:30:34_testBackupSet',
                             'zfs snapshot srcPool1/srcPool1Fs2@zipper_1977-02-01T00:00:05_testBackupSet',
                             'zfs send -P -i srcPool1/srcPool1Fs1@zipper_1932-01-01T17:30:34_testBackupSet srcPool1/srcPool1Fs2@zipper_1977-02-01T00:00:05_testBackupSet | zfs receive backupPool2/srcPool1/srcPool1Fs2@zipper_1977-02-01T00:00:05_testBackupSet'])
        self._assertRecorded(recorder,
                             ['1977-02-01T00:00:00	testBackupSet	backupPool2	error	srcPool1			BackupError	backup of srcPool1 to backupPool2/srcPool1: no common full backup snapshot, yet backup pool backupPool2 already has the file system, must specify --allowOverwrite to create a new backup	',
                              '1977-02-01T00:00:01	testBackupSet	backupPool2	full	zipper_1932-01-01T17:30:34_testBackupSet		backupPool2/srcPool1/srcPool1Fs1@zipper_1932-01-01T17:30:34_testBackupSet	50000		',
                              '1977-02-01T00:00:03	testBackupSet	backupPool2	incr	zipper_1932-01-01T17:30:34_testBackupSet	srcPool1/srcPool1Fs1@zipper_1977-02-01T00:00:02_testBackupSet	backupPool2/srcPool1/srcPool1Fs1@zipper_1977-02-01T00:00:02_testBackupSet	50000		',
                              '1977-02-01T00:00:04	testBackupSet	backupPool2	full	zipper_1932-01-01T17:30:34_testBackupSet		backupPool2/srcPool1/srcPool1Fs2@zipper_1932-01-01T17:30:34_testBackupSet	50000		',
                              '1977-02-01T00:00:06	testBackupSet	backupPool2	incr	zipper_1932-01-01T17:30:34_testBackupSet	srcPool1/srcPool1Fs2@zipper_1977-02-01T00:00:05_testBackupSet	backupPool2/srcPool1/srcPool1Fs2@zipper_1977-02-01T00:00:05_testBackupSet	50000		'])
        del recorder

    def testBackupSetToPool1Full(self):
        GmtTimeFaker.setTime("1982-02-01")
        zfs = ZfsMock()
        zfs.add(self.srcPool1, self.srcPool1Fs1)
        zfs.add(self.srcPool1, self.srcPool1Fs2)
        zfs.add(self.backupPool1)
        zfs.add(self.backupPool2Off)
        recorder = TestBackupRecorder(self.id())
        bsb = BackupSetBackup(zfs, recorder, self.backupConf1, allowOverwrite=False)
        bsb.backup()
        self._assertActions(zfs,
                            ['zfs snapshot srcPool1/srcPool1Fs1@zipper_1982-02-01T00:00:00_testBackupSet',
                             'zfs send -P srcPool1/srcPool1Fs1@zipper_1982-02-01T00:00:00_testBackupSet | zfs receive backupPool1/srcPool1/srcPool1Fs1@zipper_1982-02-01T00:00:00_testBackupSet',
                             'zfs snapshot srcPool1/srcPool1Fs2@zipper_1982-02-01T00:00:02_testBackupSet',
                             'zfs send -P srcPool1/srcPool1Fs2@zipper_1982-02-01T00:00:02_testBackupSet | zfs receive backupPool1/srcPool1/srcPool1Fs2@zipper_1982-02-01T00:00:02_testBackupSet'])
        self._assertRecorded(recorder,
                             ['1982-02-01T00:00:01	testBackupSet	backupPool1	full	srcPool1/srcPool1Fs1@zipper_1982-02-01T00:00:00_testBackupSet		backupPool1/srcPool1/srcPool1Fs1@zipper_1982-02-01T00:00:00_testBackupSet	50000		',
                              '1982-02-01T00:00:03	testBackupSet	backupPool1	full	srcPool1/srcPool1Fs2@zipper_1982-02-01T00:00:02_testBackupSet		backupPool1/srcPool1/srcPool1Fs2@zipper_1982-02-01T00:00:02_testBackupSet	50000		'])
        del recorder

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(BackupSnapshotTests))
    suite.addTest(unittest.makeSuite(BackuperTests))
    return suite

if __name__ == '__main__':
    unittest.main()
