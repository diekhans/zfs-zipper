"""
Classes to support zfs-backuper.  Some of these are configuration objects.
"""
import os,re
from enum import Enum
import logging
from .zfs import ZfsPoolHealth
from .typeops import asNameStrOrNone, asStrOrEmpty, currentGmtTimeStr
logger = logging.getLogger()

# got through level of indirection to allow tests to control time to compare to results.
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

    header = ("time", "action", "src1Snap", "src2Snap", "backupSnap", "size", "exception", "info")

    def __init__(self, recordTsvFile):
        self.recordTsvFh = open(recordTsvFile, "a", buffering=1)  # line buffered
        if self.recordTsvFh.tell() == 0:
            self.recordTsvFh.write("\t".join(self.header) + "\n")

    def record(self, action, src1Snap=None, src2Snap=None, backupSnap=None, size=None, exception=None, info=None):
        rec = (currentGmtTimeStrFunc(), action, asStrOrEmpty(src1Snap), asStrOrEmpty(src2Snap), asStrOrEmpty(backupSnap), asStrOrEmpty(size), asStrOrEmpty(exception), asStrOrEmpty(info))
        self.recordTsvFh.write("\t".join(rec) + "\n")

    def error(self, exception, src1Snap=None, src2Snap=None, backupSnap=None):
        # make sure there are no newlines or tabs
        msg = re.sub("\\s", " ", str(exception))
        self.record("error", src1Snap, src2Snap, backupSnap, type(exception).__name__, msg)

    def getFileName(self):
        return self.recordTsvFh.name
        
    def close(self):
        if self.recordTsvFh != None:
            self.recordTsvFh.close()
            self.recordTsvFh = None

    def __del__(self):
        self.close()
        
class BackupSnapshot(object):
    """Parsed backup snapshot name.  Use create methods, not constructor"
    """
    fullForm = "backuper_<GMT>_<backupset>_full_<backuppool>"
    incrForm = "backuper_<GMT>_<backupset>_incr"

    prefix = "backuper"

    @staticmethod
    def createFromSnapshotName(snapshotNameSpec, dropFileSystem=False):
        "Keep file systems name, unless drop specified"
        fileSystemName, snapshotName = BackupSnapshot.splitZfsSnapshotName(snapshotNameSpec)
        timestamp, backupsetName, backupType, backupPoolName = BackupSnapshot.__parseSnapshotName(snapshotName)
        if dropFileSystem:
            fileSystemName = None
        return BackupSnapshot(fileSystemName=fileSystemName, timestamp=timestamp, backupsetName=backupsetName, backupType=backupType, backupPoolName=backupPoolName)

    @staticmethod
    def createFromSnapshot(snapshot, fileSystem=None):
        "create from another snapshot, excluding file system, but possible setting a new one"
        return BackupSnapshot(fileSystemName=asNameStrOrNone(fileSystem),
                              timestamp=snapshot.timestamp, backupsetName=snapshot.backupsetName, backupType=snapshot.backupType, backupPoolName=snapshot.backupPoolName)

    @staticmethod
    def createCurrent(backupsetName, backupType, backupPool=None, fileSystem=None):
        "create using current timestamp.  The backupType argument can be Backtype or string."
        return BackupSnapshot(timestamp=currentGmtTimeStrFunc(), backupsetName=backupsetName, backupType=backupType, backupPoolName=asNameStrOrNone(backupPool), fileSystemName=asNameStrOrNone(fileSystem))

    def __init__(self, fileSystemName=None, timestamp=None, backupsetName=None, backupType=None, backupPoolName=None):
        "use create methods, don't call this.  The backupType argument can be Backtype or string."
        self.fileSystemName = fileSystemName
        self.timestamp = timestamp
        self.backupsetName = backupsetName
        self.backupType = BackupType.parse(backupType) if isinstance(backupType, str) else backupType
        self.backupPoolName = backupPoolName
        assert self.backupType in (BackupType.incr, BackupType.full)
        assert ((self.backupType == BackupType.full) and (self.backupPoolName != None)) or ((self.backupType == BackupType.incr) and (self.backupPoolName == None))

    def __str__(self):
        return self.getFileSystemSnapshotName()
        
    def getSnapshotName(self):
        "construct name without FS"
        return self.prefix + "_" + self.timestamp + "_" + self.backupsetName + "_" + str(self.backupType) + (("_"+self.backupPoolName) if self.backupPoolName != None else "")

    def getFileSystemSnapshotName(self):
        "construct name with FS"
        if self.fileSystemName == None:
            return self.getSnapshotName()
        else:
            return self.fileSystemName + "@" + self.getSnapshotName()

    @staticmethod
    def splitZfsSnapshotName(snapshotName):
        "parse into (filesys, name)"
        parts = snapshotName.split('@')
        if len(parts) > 2:
            raise ValueError("invalid snapshotName `%s', expected zero or one `@'" % (snapshotName,))
        if len(parts) == 2:
            return parts
        else:
            return (None, parts[0])

    @staticmethod
    def __parseSnapshotName(name):
        "parse a simple snapshot name into (timestr, backupsetName, type, backupPoolName|None)"
        parts = name.split('_')
        if not (4 <= len(parts) <= 5):
            raise ValueError("expected snapshot name in the form %s or %s, got `%s'" % (BackupSnapshot.fullForm, BackupSnapshot.incrForm, name))
        if parts[0] != BackupSnapshot.prefix:
            raise ValueError("snapshot name doesn't start with %s, got `%s'" % (BackupSnapshot.prefix, name))
        if parts[3] not in (str(BackupType.full), str(BackupType.incr)):
            raise ValueError("snapshot type isn't %s or %s, got `%s' in `%s'" % (BackupType.full, BackupType.incr, parts[3], name))
        return (parts[1], parts[2], parts[3], parts[4] if len(parts) > 4 else None)

    @staticmethod
    def isBackuperSnapshot(snapshotName):
        "is this one of ours? Maybe or my not include ZFS fs prefix"
        iBase = snapshotName.find('@')+1  # start of snapshot name, 0 if no fs in name
        return snapshotName.startswith(BackupSnapshot.prefix, iBase)

class BackupSnapshots(list):
    "list of snapshots objects from a file system, order from newest to oldest"
    def __init__(self, zfs, fileSystem=None):
        if fileSystem != None:
            self.__loadSnapshots(zfs, fileSystem)

    def __loadSnapshots(self, zfs, fileSystem):
        for zfsSnapshot in zfs.listSnapshots(fileSystem.name):
            if BackupSnapshot.isBackuperSnapshot(zfsSnapshot.name):
                self.append(BackupSnapshot.createFromSnapshotName(zfsSnapshot.name))
        self.reverse()

    def findNewestFullSnapshot(self, backupPool):
        for snapshot in self:
            if (snapshot.backupType == snapshotType) and (snapshot.backupPoolName == backupPool.name):
                return snapshot
        return None

    def findNewestCommonFull(self, otherBackupSnapshots, backupPool):
        "return snapshot of the newest common full snapshot"
        for snapshot in self:
            if (snapshot.backupType == BackupType.full) and (snapshot.backupPoolName == backupPool.name) and otherBackupSnapshots.find(snapshot.getSnapshotName()):
                return snapshot
        return None

    
    def find(self, snapshotName):
        for snapshot in self:
            if snapshot.getSnapshotName() == snapshotName:
                return snapshot
        return None

            
class BackupErrorStop(Exception):
    "error were continuing to the next backup is a bad idea "
    pass
class BackupErrorSkip(Exception):
    "error were continuing to the next backup is allowed "
    pass

class FsBackup(object):
    "backup one file system (args are objects, not names)"
    def __init__(self, zfs, backupSetConf, sourceFileSystem, backupPool, allowOverwrite):
        self.zfs = zfs
        self.backupSetConf = backupSetConf
        self.allowOverwrite = allowOverwrite

        ## backup source
        self.sourceFileSystem = sourceFileSystem
        self.sourceSnapshots = BackupSnapshots(zfs, sourceFileSystem)

        ## backup target
        self.backupPool = backupPool
        self.backupFileSystemName = backupSetConf.getBackupPool(backupPool.name).determineBackupFileSystemName(sourceFileSystem)
        # None if file system doesn't exist
        self.backupFileSystem = zfs.getFileSystem(self.backupPool, self.backupFileSystemName)
        self.backupSnapshots = BackupSnapshots(zfs, self.backupFileSystem)

    def __recordFull(self, recorder, sourceSnapshot, backupSnapshot, info):
        # full	test_src@snap1	481832
        # size	481832
        if len(info) != 2:
            raise BackupErrorStop("expected 2 lines from ZFS send|receive full, got: " + str(info))
        info0 = info[0]
        if len(info0) != 3:
            raise BackupErrorStop("expected 3 columns in ZFS send|receive full record, got: " + str(info0))
        if info0[0] != "full":
            raise BackupErrorStop("expected 'full' in column 0 of ZFS send|receive full record, got: " + str(info0))
        # FIXME:
        # if info0[1] != sourceSnapshot.getFileSystemSnapshotName():
        #     raise BackupErrorStop("expected snapshot name '" + sourceSnapshot.getFileSystemSnapshotName() + "' in column 1 of ZFS send|receive full record, got: " + str(info0))
        recorder.record("full", sourceSnapshot.getFileSystemSnapshotName(), None, backupSnapshot.getFileSystemSnapshotName(), info0[2])
        
    def __backupFull(self, recorder):
        if (len(self.backupSnapshots) > 0) and not self.allowOverwrite:
            raise BackupErrorSkip("skip backup of %s to %s: full backup snapshots exists and overwrite not specified" %
                                  (self.sourceFileSystem.name, self.backupFileSystemName))
        sourceSnapshot = BackupSnapshot.createCurrent(self.backupSetConf.name, BackupType.full, backupPool=self.backupPool, fileSystem=self.sourceFileSystem)
        backupSnapshot = BackupSnapshot.createFromSnapshot(sourceSnapshot, self.backupFileSystemName)
        logger.info("backup snapshot %s -> %s" % (sourceSnapshot, backupSnapshot))
        self.zfs.createSnapshot(sourceSnapshot.getFileSystemSnapshotName())
        info = self.zfs.sendRecvFull(sourceSnapshot.getFileSystemSnapshotName(), backupSnapshot.getFileSystemSnapshotName(), self.allowOverwrite)
        self.__recordFull(recorder, sourceSnapshot, backupSnapshot, info)

    def __buildIncrStapshotList(self):
        """build list of source snapshots that should be backed in the incremental backup.  The
        first will be the latest common, the rest will not be in the backup.  It will not include
        full backups for the other pool."""
        incrSourceSnapshots = []
        for sourceSnapshot in self.sourceSnapshots:
            if (sourceSnapshot.backupType == BackupType.incr) or (sourceSnapshot.backupPoolName == self.backupPool.name):
                incrSourceSnapshots.insert(0, sourceSnapshot)
                if self.backupSnapshots.find(sourceSnapshot.getSnapshotName()) != None:
                    return incrSourceSnapshots  # found the common one
        # this shouldn't happen, should have been detected by  __checkForNewPoolForIncr
        raise BackupErrorStop("BUG backup of %s to %s: can't find common snapshot" %
                              (self.sourceFileSystem.name, self.backupFileSystemName))

    def __recordIncr(self, recorder, prevSourceSnapshot, sourceSnapshot, backupSnapshot, info):
        # incremental	snap1	test_src@snap2	593632
        # size	481832
        if len(info) != 2:
            raise BackupErrorStop("expected 2 lines from ZFS send|receive incremental, got: " + str(info))
        info0 = info[0]
        if len(info0) != 4:
            raise BackupErrorStop("expected 4 columns in ZFS send|receive incremental record, got: " + str(info0))
        if info0[0] != "incremental":
            raise BackupErrorStop("expected 'incremental' in column 0 of ZFS send|receive incremental record, got: " + str(info0))
        # FIXME:
        # if info0[1] != prevSourceSnapshot.getSnapshotName():
        #     raise BackupErrorStop("expected snapshot name '" + prevSourceSnapshot.getSnapshotName() + "' in column 1 of ZFS send|receive full record, got: " + str(info0))
        # if info0[2] != sourceSnapshot.getFileSystemSnapshotName():
        #     raise BackupErrorStop("expected snapshot name '" + sourceSnapshot.getFileSystemSnapshotName() + "' in column 2 of ZFS send|receive full record, got: " + str(info0))
        recorder.record("incr", prevSourceSnapshot.getFileSystemSnapshotName(), sourceSnapshot.getFileSystemSnapshotName(), backupSnapshot.getFileSystemSnapshotName(), info0[2])
        
    def __makeIncrBackup(self, recorder, prevSourceSnapshot, sourceSnapshot):
        backupSnapshot = BackupSnapshot.createFromSnapshot(sourceSnapshot, self.backupFileSystem)
        logger.info("backup snapshot %s..%s -> %s" % (prevSourceSnapshot, sourceSnapshot, backupSnapshot))
        info = self.zfs.sendRecvIncr(prevSourceSnapshot.getFileSystemSnapshotName(), sourceSnapshot.getFileSystemSnapshotName(), backupSnapshot.getFileSystemSnapshotName())
        self.__recordIncr(recorder, prevSourceSnapshot, sourceSnapshot, backupSnapshot, info)
        
    def __backupIncrMissing(self, recorder):
        "backup all existing source snapshots that are not on backup, return latest"
        incrSourceSnapshots = self.__buildIncrStapshotList()
        for iSrc in xrange(1, len(incrSourceSnapshots)):
            self.__makeIncrBackup(recorder, incrSourceSnapshots[iSrc-1], incrSourceSnapshots[iSrc])
        return incrSourceSnapshots[-1]

    def __backupIncr(self, recorder):
        prevSourceSnapshot = self.__backupIncrMissing(recorder)
        sourceSnapshot = BackupSnapshot.createCurrent(self.backupSetConf.name, BackupType.incr, fileSystem=self.sourceFileSystem)
        self.zfs.createSnapshot(sourceSnapshot.getFileSystemSnapshotName())
        self.__makeIncrBackup(recorder, prevSourceSnapshot, sourceSnapshot)

    def __checkForNewPoolForIncr(self):
        """check to see if we are doing an incremental, however the pool doesn't have a common
        full.  If the pool doesn't have the file system, then we will do a full.  Otherwise,
        allowOverwrite must have been specified"""
        haveCommonFull = (self.backupSnapshots.findNewestCommonFull(self.sourceSnapshots, self.backupPool) != None)
        if haveCommonFull:
            return False  # have a common full, no need to force full
        elif (self.backupFileSystem != None) and not self.allowOverwrite:
            raise BackupErrorSkip("skip incremental backup of %s to %s: no common full backup snapshot, backup pool %s already has the file system, must specify allowOverwrite to create a new full backup" %
                                  (self.sourceFileSystem.name, self.backupFileSystemName, self.backupPool.name))
        else:
            return True

    def backup(self, recorder, backupType):
        logger.info("backup: %s  backupSet %s  %s -> %s overwrite:%s" %
                    (backupType, self.backupSetConf.name, self.sourceFileSystem.name, self.backupFileSystemName, str(self.allowOverwrite)))
        try:
            # do a full if requested or if there are no full on the backup
            if (backupType == BackupType.full) or self.__checkForNewPoolForIncr():
                self.__backupFull(recorder)
            else:
                self.__backupIncr(recorder)
        except Exception, ex:
            logger.exception("backup of %s to %s failed" % (self.backupSetConf.name, self.sourceFileSystem.name))
            recorder.error(ex, self.sourceFileSystem.name)
            raise

class BackupSetBackup(object):
    "backup of all data in a backup set"
    def __init__(self, zfs, recorder, backupSetConf, allowOverwrite):
        self.recorder = recorder
        self.zfs = zfs
        self.backupSetConf = backupSetConf
        self.allowOverwrite = allowOverwrite
        self.backupPool = self.__findBackupPoolToUse()

    def __findBackupPoolToUse(self):
        onlineBackupPools = []
        for backupPoolConf in self.backupSetConf.backupPools:
            backupPool = self.__lookupOnlinePool(backupPoolConf.name)
            if backupPool != None:
                onlineBackupPools.append(backupPool)
        if len(onlineBackupPools) == 0:
            raise BackupErrorStop("no backup pool is online for backupset " + self.backupSetConf.name + ": in " + ",".join([backupPoolConf.name for backupPoolConf in self.backupPoolConf.backupPools]))
        elif len(onlineBackupPools) > 1:
            raise BackupErrorStop("multiple backup pool is online for backupset " + self.backupSetConf.name + ": in " + ",".join([backupPool.name for backupPool in onlineBackupPools]))
        else:
            return onlineBackupPools[0]

    def __lookupOnlinePool(self, poolName):
        backupPool = self.zfs.getPool(poolName)
        if backupPool == None:
            raise BackupErrorStop("unknown back pool in conf: " + backupPoolConf.name)
        if backupPool.health == ZfsPoolHealth.ONLINE:
            return backupPool
        else:
            return None

    def __getSourceFileSystem(self, sourceFileSystemConf):
        sourceFileSystem = self.zfs.getFileSystem(sourceFileSystemConf.pool, sourceFileSystemConf.name)
        if sourceFileSystem == None:
            raise BackupErrorStop("configured file system not in ZFS: " + sourceFileSystemConf.name)
        return sourceFileSystem

    def __fsBackup(self, backupType, sourceFileSystemConf):
        try:
            sourceFileSystem = self.__getSourceFileSystem(sourceFileSystemConf)
            fsBackup = FsBackup(self.zfs, self.backupSetConf, sourceFileSystem, self.backupPool, self.allowOverwrite)
        except Exception, ex:
            self.recorder.error(ex)
            raise
        fsBackup.backup(self.recorder, backupType)

    def backupAll(self, backupType):
        for sourceFileSystemConf in self.backupSetConf.sourceFileSystems:
            self.__fsBackup(backupType, sourceFileSystemConf)

    def __getSourceFileSystemConf(self, sourceFileSystemName):
        sourceFileSystemConf = self.backupSetConf.getSourceFileSystem(sourceFileSystemName)
        if sourceFileSystemConf == None:
            raise BackupErrorStop("file system not specified in conf: " + sourceFileSystemName)
        return sourceFileSystemConf
        
    def backupOne(self, backupType, sourceFileSystemName):
        try:
            sourceFileSystemConf = self.__getSourceFileSystemConf(sourceFileSystemName)
        except Exception, ex:
            self.recorder.error(ex)
            raise
        self.__fsBackup(backupType, sourceFileSystemConf)
