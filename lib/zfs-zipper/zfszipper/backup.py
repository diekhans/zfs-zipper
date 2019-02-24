"""
Classes to support zfs-zipper.  Some of these are configuration objects.
"""
import os
import re
import logging
import time
from enum import Enum
from .zfs import ZfsPoolHealth
from .typeops import asNameStrOrNone, asStrOrEmpty, currentGmtTimeStr
logger = logging.getLogger()

# get through level of indirection to allow tests to control time to compare to results.
currentGmtTimeStrFunc = currentGmtTimeStr

# type of backup
class BackupType(Enum):
    full = 0
    incr = 1

    def __str__(self):
        return "full" if self == BackupType.full else "incr"

    @staticmethod
    def parse(strvalue):
        if strvalue == "full":
            return BackupType.full
        elif strvalue == "incr":
            return BackupType.incr
        else:
            raise ValueError('expected backup type of "full" or "incr", got "' + str(strvalue) + '"')

class BackupRecorder(object):
    "record history of backups in a file"

    header = ("time", "backupSet", "backupPool", "action", "src1Snap", "src2Snap", "backupSnap", "size", "exception", "info")

    def __init__(self, recordTsvFile, outFh=None):
        "if recordTsvFile or outFh can be  None made"
        self.recordTsvFh = None
        self.outFh = outFh
        if recordTsvFile is not None:
            if not os.path.exists(os.path.dirname(recordTsvFile)):
                os.makedirs(os.path.dirname(recordTsvFile))
            self.recordTsvFh = open(recordTsvFile, "a", buffering=1)  # line buffered
        self._writeHeader()

    def _writeHeader(self):
        headerLine = "\t".join(self.header) + "\n"
        if (self.recordTsvFh is not None) and (self.recordTsvFh.tell() == 0):
            self.recordTsvFh.write(headerLine)  # file is empty, write header
        if self.outFh is not None:
            self.outFh.write(headerLine)

    def record(self, backupSet, backupPool, action, src1Snap=None, src2Snap=None, backupSnap=None, size=None, exception=None, info=None):
        rec = (currentGmtTimeStrFunc(), asNameStrOrNone(backupSet), asNameStrOrNone(backupPool), action, asStrOrEmpty(src1Snap), asStrOrEmpty(src2Snap), asStrOrEmpty(backupSnap), asStrOrEmpty(size), asStrOrEmpty(exception), asStrOrEmpty(info))
        line = "\t".join(rec) + "\n"
        if self.recordTsvFh is not None:
            self.recordTsvFh.write(line)
        if self.outFh is not None:
            self.outFh.write(line)
            self.outFh.flush()

    def error(self, backupSet, backupPool, exception, src1Snap=None, src2Snap=None, backupSnap=None):
        # make sure there are no newlines or tabs
        msg = re.sub("\\s", " ", str(exception))
        self.record(backupSet, backupPool, "error", src1Snap, src2Snap, backupSnap, type(exception).__name__, msg)

    def getFileName(self):
        if self.recordTsvFh is not None:
            return self.recordTsvFh.name
        else:
            return None

    def close(self):
        if self.recordTsvFh is not None:
            self.recordTsvFh.close()
            self.recordTsvFh = None

    def __del__(self):
        self.close()

class BackupSnapshot(object):
    """Parsed backup snapshot name.  Use create methods, not constructor"
    """
    # backup sent names may name contain `_'
    fullForm = "zipper_<GMT>_<backupset>_full"
    incrForm = "zipper_<GMT>_<backupset>_incr"
    # re group:          1       2        3

    prefix = "zipper_"
    snapshotNameRe = re.compile("^" + prefix + "([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})_(.+)_(full|incr)$")

    @staticmethod
    def createFromSnapshotName(snapshotNameSpec, dropFileSystem=False, requireFileSystem=False):
        "Keep file systems name, unless drop specified"
        fileSystemName, snapshotName = BackupSnapshot.splitZfsSnapshotName(snapshotNameSpec)
        timestamp, backupsetName, backupType = BackupSnapshot._parseSnapshotName(snapshotName)
        if requireFileSystem and (fileSystemName is None):
            raise Exception("file system name requred in snapshopt: {}".format(snapshotNameSpec))
        if dropFileSystem:
            fileSystemName = None
        if fileSystemName is not None:
            fileSystemName = os.path.normpath(fileSystemName)
        return BackupSnapshot(fileSystemName=fileSystemName, timestamp=timestamp, backupsetName=backupsetName, backupType=backupType)

    @staticmethod
    def createFromSnapshot(snapshot, fileSystem=None):
        "create from another snapshot, excluding file system, but possible setting a new one"
        return BackupSnapshot(fileSystemName=asNameStrOrNone(fileSystem),
                              timestamp=snapshot.timestamp, backupsetName=snapshot.backupsetName, backupType=snapshot.backupType)

    @staticmethod
    def createCurrent(backupsetName, backupType, backupPool=None, fileSystem=None):
        "create using current timestamp.  The backupType argument can be Backtype or string."
        # Ouch, had the problem that the test cases ran so quickly that
        # the one second resoltion of the time can result in snampshot name
        # collision. to addess, with just sleep to force them to be unique.
        time.sleep(2)
        return BackupSnapshot(timestamp=currentGmtTimeStrFunc(), backupsetName=backupsetName, backupType=backupType,
                              fileSystemName=asNameStrOrNone(fileSystem))

    def __init__(self, fileSystemName=None, timestamp=None, backupsetName=None, backupType=None):
        "use create methods, don't call this.  The backupType argument can be Backtype or string."
        self.fileSystemName = fileSystemName
        self.timestamp = timestamp
        self.backupsetName = backupsetName
        self.backupType = BackupType.parse(backupType) if isinstance(backupType, str) else backupType
        assert self.backupType in (BackupType.incr, BackupType.full)

    def __str__(self):
        return self.getFileSystemSnapshotName()

    def getSnapshotName(self):
        "construct name without FS"
        return self.prefix + self.timestamp + "_" + self.backupsetName + "_" + str(self.backupType)

    def getFileSystemSnapshotName(self):
        "construct name with FS"
        if self.fileSystemName is None:
            return self.getSnapshotName()
        else:
            return self.fileSystemName + "@" + self.getSnapshotName()

    @staticmethod
    def splitZfsSnapshotName(snapshotName):
        "parse into (filesys, name)"
        parts = snapshotName.split('@')
        if len(parts) > 2:
            raise ValueError("invalid snapshotName '{}', expected zero or one `@'".format(snapshotName))
        if len(parts) == 2:
            return parts
        else:
            return (None, parts[0])

    @staticmethod
    def _parseSnapshotName(name):
        "parse a simple snapshot name into (timestr, backupsetName, type)"
        if not name.startswith(BackupSnapshot.prefix):
            raise ValueError("snapshot name doesn't start with '{}', got '{}'".format(BackupSnapshot.prefix, name))
        parse = BackupSnapshot.snapshotNameRe.match(name)
        if parse is None:
            raise ValueError("expected snapshot name in the form '{}' or '{}, got '{}'".format(BackupSnapshot.fullForm, BackupSnapshot.incrForm, name))
        timestr = parse.group(1)
        backupSetName = parse.group(2)
        backupType = parse.group(3)
        return (timestr, backupSetName, backupType)

    @staticmethod
    def isZipperSnapshot(snapshotName):
        "is this one of ours? Maybe or my not include ZFS fs prefix"
        iBase = snapshotName.find('@') + 1  # start of snapshot name, 0 if no fs in name
        return snapshotName.startswith(BackupSnapshot.prefix, iBase)

class BackupSnapshots(list):
    "list of snapshots objects from a file system, order from newest to oldest"
    def __init__(self, zfs, fileSystem=None):
        if fileSystem is not None:
            self._loadSnapshots(zfs, fileSystem)

    def _loadSnapshots(self, zfs, fileSystem):
        for zfsSnapshot in zfs.listSnapshots(fileSystem.name):
            if BackupSnapshot.isZipperSnapshot(zfsSnapshot.name):
                self.append(BackupSnapshot.createFromSnapshotName(zfsSnapshot.name))
        self.reverse()

    def findNewestCommonFull(self, otherBackupSnapshots):
        "return snapshot of the newest common full snapshot"
        for snapshot in self:
            if (snapshot.backupType == BackupType.full) and (otherBackupSnapshots.find(snapshot.getSnapshotName()) is not None):
                return snapshot
        return None

    def findIdx(self, snapshotName):
        for idx in range(len(self)):
            if self[idx].getSnapshotName() == snapshotName:
                return idx
        return -1

    def find(self, snapshotName):
        idx = self.findIdx(snapshotName)
        return self[idx] if idx >= 0 else None

class BackupError(Exception):
    "Backup error"
    pass

class FsBackup(object):
    "backup one file system (args are objects, not names)"
    def __init__(self, zfs, recorder, backupSetConf, sourceFileSystem, backupPool, allowOverwrite):
        self.zfs = zfs
        self.recorder = recorder
        self.backupSetConf = backupSetConf
        self.allowOverwrite = allowOverwrite

        # backup source
        self.sourceFileSystem = sourceFileSystem
        self.sourceSnapshots = BackupSnapshots(zfs, sourceFileSystem)

        # backup target
        self.backupPool = backupPool
        self.backupFileSystemName = backupSetConf.getBackupPoolConf(backupPool.name).determineBackupFileSystemName(sourceFileSystem)

        # None if file system doesn't exist
        self.backupFileSystem = zfs.getFileSystem(self.backupFileSystemName)
        self.backupSnapshots = BackupSnapshots(zfs, self.backupFileSystem)

    def _recordFull(self, sourceSnapshot, backupSnapshot, info):
        # FIXME: create common code for parsing input; note incremental can get 3 or 4 columns
        # full	test_src@snap1	481832
        # size	481832
        if len(info) != 2:
            raise BackupError("expected 2 lines from ZFS send|receive full, got: " + str(info))
        info0 = info[0]
        if len(info0) != 3:
            raise BackupError("expected 3 columns in ZFS send|receive full record, got: " + str(info0))
        if info0[0] != "full":
            raise BackupError("expected 'full' in column 0 of ZFS send|receive full record, got: " + str(info0))
        self.recorder.record(self.backupSetConf, self.backupPool, "full", sourceSnapshot.getFileSystemSnapshotName(), None, backupSnapshot.getFileSystemSnapshotName(), info0[2])

    def _backupFull(self):
        if (len(self.backupSnapshots) > 0) and not self.allowOverwrite:
            raise BackupError("backup of {} to {}: full backup snapshots exists and overwrite not specified"
                              .format(self.sourceFileSystem.name, self.backupFileSystemName))

        sourceSnapshot = BackupSnapshot.createCurrent(self.backupSetConf.name, BackupType.full, backupPool=self.backupPool, fileSystem=self.sourceFileSystem)
        backupSnapshot = BackupSnapshot.createFromSnapshot(sourceSnapshot, self.backupFileSystemName)
        logger.info("backup snapshot {} -> {}".format(sourceSnapshot, backupSnapshot))
        self.zfs.createSnapshot(sourceSnapshot.getFileSystemSnapshotName())
        info = self.zfs.sendRecvFull(sourceSnapshot.getFileSystemSnapshotName(), backupSnapshot.getFileSystemSnapshotName(), self.allowOverwrite)
        self._recordFull(sourceSnapshot, backupSnapshot, info)

    def _buildIncrStapshotList(self):
        """build list of source snapshots that should be backed in the incremental backup.  The
        first will be the latest common, the rest will not be in the backup."""
        sourceSnapshots = []
        for sourceSnapshot in self.sourceSnapshots:
            sourceSnapshots.insert(0, sourceSnapshot)
            if self.backupSnapshots.find(sourceSnapshot.getSnapshotName()) is not None:
                return sourceSnapshots  # found the common one
        # this shouldn't happen, should have been detected by  _checkForNewPoolForIncr
        raise BackupError("BUG backup of {} to {}: can't find common snapshot"
                          .format(self.sourceFileSystem.name, self.backupFileSystemName))

    def _recordIncr(self, prevSourceSnapshot, sourceSnapshot, backupSnapshot, info):
        # incremental	snap1	test_src@snap2	593632
        # size	481832
        if len(info) != 2:
            raise BackupError("expected 2 lines from ZFS send|receive incremental, got: " + str(info))
        info0 = info[0]
        if not (3 <= len(info0) <= 4):
            raise BackupError("expected 3-4 columns in ZFS send|receive incremental record, got: " + str(info0))
        if info0[0] != "incremental":
            raise BackupError("expected 'incremental' in column 0 of ZFS send|receive incremental record, got: " + str(info0))
        self.recorder.record(self.backupSetConf, self.backupPool, "incr", prevSourceSnapshot.getFileSystemSnapshotName(), sourceSnapshot.getFileSystemSnapshotName(), backupSnapshot.getFileSystemSnapshotName(), info0[2])

    def _makeIncrBackup(self, prevSourceSnapshot, sourceSnapshot):
        backupSnapshot = BackupSnapshot.createFromSnapshot(sourceSnapshot, self.backupFileSystem)
        logger.info("backup snapshot {}..{} -> {}".format(prevSourceSnapshot, sourceSnapshot, backupSnapshot))
        info = self.zfs.sendRecvIncr(prevSourceSnapshot.getFileSystemSnapshotName(), sourceSnapshot.getFileSystemSnapshotName(), backupSnapshot.getFileSystemSnapshotName())
        self._recordIncr(prevSourceSnapshot, sourceSnapshot, backupSnapshot, info)

    def _backupIncrMissing(self):
        "backup all existing source snapshots that are not on backup, return latest"
        incrSourceSnapshots = self._buildIncrStapshotList()
        for iSrc in range(1, len(incrSourceSnapshots)):
            self._makeIncrBackup(incrSourceSnapshots[iSrc - 1], incrSourceSnapshots[iSrc])
        return incrSourceSnapshots[-1]

    def _backupIncr(self):
        prevSourceSnapshot = self._backupIncrMissing()
        sourceSnapshot = BackupSnapshot.createCurrent(self.backupSetConf.name, BackupType.incr, fileSystem=self.sourceFileSystem)
        self.zfs.createSnapshot(sourceSnapshot.getFileSystemSnapshotName())
        self._makeIncrBackup(prevSourceSnapshot, sourceSnapshot)

    def _checkForNewPoolForIncr(self):
        """Check to see if we are doing an incremental and the pool doesn't have a common
        full.  If the pool doesn't have the file system, then we will do a full.  Otherwise,
        allowOverwrite must have been specified"""
        haveCommonFull = (self.backupSnapshots.findNewestCommonFull(self.sourceSnapshots) is not None)
        if haveCommonFull:
            return False  # have a common full, no need to force full
        elif (self.backupFileSystem is not None) and not self.allowOverwrite:
            raise BackupError("incremental backup of {} to {}: no common full backup snapshot, backup pool {} already has the file system, must specify allowOverwrite to create a new full backup"
                              .format(self.sourceFileSystem.name, self.backupFileSystemName, self.backupPool.name))
        else:
            return True  # force full

    def _backup(self, backupType):
        # do a full if requested or if there are no full on the backup
        if (backupType == BackupType.full) or self._checkForNewPoolForIncr():
            self._backupFull()
        else:
            self._backupIncr()

    def backup(self, backupType):
        logger.info("backup: {} backupSet {} {} -> {} overwrite: {}"
                    .format(backupType, self.backupSetConf.name, self.sourceFileSystem.name, self.backupFileSystemName, str(self.allowOverwrite)))
        try:
            self._backup(backupType)
        except Exception as ex:
            logger.exception("backup of {} to {} failed"
                             .format(self.backupSetConf.name, self.sourceFileSystem.name))
            self.recorder.error(self.backupSetConf, self.backupPool, ex, self.sourceFileSystem.name)
            raise

class BackupSetBackup(object):
    "backup of all data in a backup set"
    def __init__(self, zfs, recorder, backupSetConf, allowOverwrite):
        self.recorder = recorder
        self.zfs = zfs
        self.backupSetConf = backupSetConf
        self.allowOverwrite = allowOverwrite
        self.backupPool, self.importedPool = self._findBackupPoolToUse()

    def _findBackupPoolToUse(self):
        pool = self._getImportedPool()
        if pool is not None:
            return pool, False
        pool = self._getExportedPool()
        if pool is not None:
            return pool, True
        raise BackupError("no backup pool is imported or ready for import for backupset {} in {}"
                          .format(self.backupSetConf.name, self.backupSetConf.backupPoolNames))

    def _getExportedPool(self):
        pools = self._getExportedPools()
        if len(pools) == 0:
            return None
        elif len(pools) == 1:
            return pools[0]
        else:
            raise BackupError("multiple backup pools are exported for backupset {} in {}"
                              .format(self.backupSetConf.name, [pool.name for pool in pools]))

    def _getExportedPools(self):
        pools = []
        for pool in self.zfs.listExported():
            if (pool.name in self.backupSetConf.backupPoolNames) and (pool.health == ZfsPoolHealth.ONLINE):
                pools.append(pool)
        return pools

    def _getImportedPool(self):
        pools = self._getImportedPools()
        if len(pools) == 0:
            return None
        elif len(pools) == 1:
            return pools[0]
        else:
            raise BackupError("multiple backup pools are imported for backupset {} in {}"
                              .format(self.backupSetConf.name, [pool.name for pool in pools]))

    def _getImportedPools(self):
        pools = []
        for backupPoolConf in self.backupSetConf.backupPoolConfs:
            pool = self._lookupImportedPool(backupPoolConf.name)
            if pool is not None:
                pools.append(pool)
        return pools

    def _lookupImportedPool(self, poolName):
        backupPool = self.zfs.getPool(poolName)
        if (backupPool is not None) and (backupPool.health == ZfsPoolHealth.ONLINE):
            return backupPool
        else:
            return None

    def _getSourceFileSystem(self, sourceFileSystemConf):
        sourceFileSystem = self.zfs.getFileSystem(sourceFileSystemConf.name)
        if sourceFileSystem is None:
            raise BackupError("configured file system not in ZFS: " + sourceFileSystemConf.name)
        return sourceFileSystem

    def _fsBackup(self, backupType, sourceFileSystemConf):
        try:
            fsBackup = FsBackup(self.zfs, self.recorder, self.backupSetConf,
                                self._getSourceFileSystem(sourceFileSystemConf),
                                self.backupPool, self.allowOverwrite)
            fsBackup.backup(backupType)
        except Exception as ex:
            self.recorder.error(self.backupSetConf, self.backupPool, ex)
            raise

    def _setupPool(self):
        if self.importedPool:
            self.zfs.importPool(self.backupPool)

    def _freeupPool(self):
        if self.importedPool:
            self.zfs.exportPool(self.backupPool)

    def backupAll(self, backupType):
        self._setupPool()
        try:
            for sourceFileSystemConf in self.backupSetConf.sourceFileSystemConfs:
                self._fsBackup(backupType, sourceFileSystemConf)
        finally:
            self._freeupPool()

    def backupOne(self, backupType, sourceFileSystemConf):
        self._setupPool()
        try:
            self._fsBackup(backupType, sourceFileSystemConf)
        finally:
            self._freeupPool()
