"""
Classes to support zfs-zipper.  Some of these are configuration objects.
"""
import os
import re
import logging
from .zfs import ZfsPoolHealth
from .snapshots import BackupSnapshot, BackupSnapshots
from .typeOps import asNameStrOrNone, asStrOrEmpty, currentGmtTimeStr
logger = logging.getLogger()

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
        rec = (currentGmtTimeStr(), asNameStrOrNone(backupSet), asNameStrOrNone(backupPool), action, asStrOrEmpty(src1Snap), asStrOrEmpty(src2Snap), asStrOrEmpty(backupSnap), asStrOrEmpty(size), asStrOrEmpty(exception), asStrOrEmpty(info))
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

class BackupError(Exception):
    "Backup error"
    pass

class FsBackup(object):
    "backup one file system (args are objects, not names)"
    def __init__(self, zfs, recorder, backupSetConf, sourceFileSystem, backupPool, allowOverwrite, *, forceOverwrite=False):
        assert(not forceOverwrite)   # no implemented
        assert(allowOverwrite if forceOverwrite else True)
        self.zfs = zfs
        self.recorder = recorder
        self.backupSetConf = backupSetConf
        self.allowOverwrite = allowOverwrite
        self.forceOverwrite = forceOverwrite

        # backup source
        self.sourceFileSystem = sourceFileSystem
        self.sourceSnapshots = BackupSnapshots(zfs, sourceFileSystem)

        # backup target
        self.backupPool = backupPool
        self.backupFileSystemName = backupSetConf.getBackupPoolConf(backupPool.name).determineBackupFileSystemName(sourceFileSystem)

        # None if file system doesn't exist
        self.backupFileSystem = zfs.findFileSystem(self.backupFileSystemName)
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
        self.recorder.record(self.backupSetConf, self.backupPool, "full",
                             src1Snap=sourceSnapshot.getFileSystemSnapshotName(),
                             backupSnap=backupSnapshot.getFileSystemSnapshotName(),
                             size=info0[2])

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
        self.recorder.record(self.backupSetConf, self.backupPool, "incr",
                             src1Snap=prevSourceSnapshot.getFileSystemSnapshotName(),
                             src2Snap=sourceSnapshot.getFileSystemSnapshotName(),
                             backupSnap=backupSnapshot.getFileSystemSnapshotName(),
                             size=info0[3])

    def _sendFull(self, sourceSnapshot):
        backupSnapshot = BackupSnapshot.createFromSnapshot(sourceSnapshot, self.backupFileSystemName)
        logger.info("send full snapshot {} -> {}".format(sourceSnapshot, backupSnapshot))
        info = self.zfs.sendRecvFull(sourceSnapshot.getFileSystemSnapshotName(), backupSnapshot.getFileSystemSnapshotName(), self.allowOverwrite)
        self._recordFull(sourceSnapshot, backupSnapshot, info)
        return backupSnapshot

    def _sendIncr(self, prevSourceSnapshot, sourceSnapshot):
        backupSnapshot = BackupSnapshot.createFromSnapshot(sourceSnapshot, self.backupFileSystemName)
        logger.info("send incr snapshot {}..{} -> {}".format(prevSourceSnapshot, sourceSnapshot, backupSnapshot))
        info = self.zfs.sendRecvIncr(prevSourceSnapshot.getFileSystemSnapshotName(), sourceSnapshot.getFileSystemSnapshotName(), backupSnapshot.getFileSystemSnapshotName())
        self._recordIncr(prevSourceSnapshot, sourceSnapshot, backupSnapshot, info)
        return backupSnapshot

    def _createSourceSnapshot(self):
        newSourceSnapshot = BackupSnapshot.createCurrent(self.backupSetConf.name, fileSystem=self.sourceFileSystem)
        logger.info("create source snapshot {}".format(newSourceSnapshot))
        self.zfs.createSnapshot(newSourceSnapshot.getFileSystemSnapshotName())
        return newSourceSnapshot

    def _backupNewFirst(self):
        "there are no source snapshots, so we just make a new snapshot and sent the whole thing"
        self._sendFull(self._createSourceSnapshot())

    def _backupNewExisting(self):
        "there are existing source snapshots, so we send them all then make a new one as an incremental"
        self._sendFull(self.sourceSnapshots[0])
        for i in range(1, len(self.sourceSnapshots)):
            self._sendIncr(self.sourceSnapshots[i - 1], self.sourceSnapshots[1])
        self._sendIncr(self.sourceSnapshots[-1], self._createSourceSnapshot())

    def _backupNew(self):
        """create a backup where there are no existing backup snapshots to start against"""
        if len(self.sourceSnapshots) == 0:
            self._backupNewFirst()
        else:
            self._backupNewExisting()

    def _backupIncrExisting(self, newestCommonSourceSnapshot):
        """sync all existing source snapshot as incrementals, starting with
        the newestCommonSourceSnapshot.  Return the final new common snapshot"""
        commonSourceIdx = self.sourceSnapshots.getIdx(newestCommonSourceSnapshot)
        for sourceIdx in range(commonSourceIdx, 0, -1):
            self._sendIncr(self.sourceSnapshots[sourceIdx], self.sourceSnapshots[sourceIdx - 1])
        return self.sourceSnapshots[0]

    def _backupIncr(self, newestCommonSourceSnapshot):
        # back up all snapshots from common point to newest
        newestCommonSourceSnapshot = self._backupIncrExisting(newestCommonSourceSnapshot)
        self._sendIncr(newestCommonSourceSnapshot, self._createSourceSnapshot())

    def _checkFullOverwrite(self):
        """Safety check checks for doing backup when there are no common snapshots """
        if (not self.allowOverwrite) and (self.backupFileSystem is not None):
            raise BackupError("backup of {} to {}: no common full backup snapshot, yet backup pool {} already has the file system, must specify --allowOverwrite to create a new backup"
                              .format(self.sourceFileSystem.name, self.backupFileSystemName, self.backupPool.name))

    def _backup(self):
        newestCommonSourceSnapshot = self.sourceSnapshots.findNewestCommon(self.backupSnapshots)
        if (newestCommonSourceSnapshot is None) or (self.forceOverwrite):
            self._checkFullOverwrite()
            self._backupNew()
        else:
            self._backupIncr(newestCommonSourceSnapshot)

    def backup(self):
        logger.info("backup: backupSet {} {} -> {} overwrite: {}"
                    .format(self.backupSetConf.name, self.sourceFileSystem.name, self.backupFileSystemName, str(self.allowOverwrite)))
        try:
            self._backup()
        except Exception as ex:
            logger.exception("backup of {} to {} failed"
                             .format(self.backupSetConf.name, self.sourceFileSystem.name))
            self.recorder.error(self.backupSetConf, self.backupPool, ex, self.sourceFileSystem.name)
            raise

class BackupSetBackup(object):
    "backup of all data in a backup set"
    def __init__(self, zfs, recorder, backupSetConf, allowOverwrite, *, forceOverwrite=False):
        self.recorder = recorder
        self.zfs = zfs
        self.backupSetConf = backupSetConf
        self.allowOverwrite = allowOverwrite
        self.forceOverwrite = forceOverwrite
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
        backupPool = self.zfs.findPool(poolName)
        if (backupPool is not None) and (backupPool.health == ZfsPoolHealth.ONLINE):
            return backupPool
        else:
            return None

    def _getSourceFileSystem(self, sourceFileSystemConf):
        sourceFileSystem = self.zfs.findFileSystem(sourceFileSystemConf.name)
        if sourceFileSystem is None:
            raise BackupError("configured file system not in ZFS: " + sourceFileSystemConf.name)
        return sourceFileSystem

    def _fsBackup(self, sourceFileSystemConf):
        try:
            fsBackup = FsBackup(self.zfs, self.recorder, self.backupSetConf,
                                self._getSourceFileSystem(sourceFileSystemConf),
                                self.backupPool, self.allowOverwrite, forceOverwrite=self.forceOverwrite)
            fsBackup.backup()
        except Exception as ex:
            self.recorder.error(self.backupSetConf, self.backupPool, ex)
            raise

    def _setupPool(self):
        if self.importedPool:
            self.zfs.importPool(self.backupPool)

    def _freeupPool(self):
        if self.importedPool:
            self.zfs.exportPool(self.backupPool)

    def backup(self, sourceFileSystemConfs=None):
        """specifying sourceFileSystemConfs can limit the file systems backed
        up to a subset."""
        if sourceFileSystemConfs is None:
            sourceFileSystemConfs = self.backupSetConf.sourceFileSystemConfs
        self._setupPool()
        try:
            for sourceFileSystemConf in sourceFileSystemConfs:
                self._fsBackup(sourceFileSystemConf)
        finally:
            self._freeupPool()
