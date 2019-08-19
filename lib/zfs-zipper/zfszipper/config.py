"""
Configuration objects.
"""
import os
import time
from collections import OrderedDict
from zfszipper import loggingOps

class BackupConfigError(Exception):
    pass

class SourceFileSystemConf(object):
    "a file system to backup, full ZFS file system name"
    def __init__(self, name):
        self.name = os.path.normpath(name)

class BackupPoolConf(object):
    "Configuration of a backup pool"
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name

    def determineBackupFileSystemName(self, fileSystem):
        """determine ZFS fileSystemName used to backup fileSystem (file systems can be name or zfs.FileSystem)"""
        return os.path.normpath(self.name + "/" + (fileSystem if isinstance(fileSystem, str) else fileSystem.name))

class BackupSetConf(object):
    """Configuration of a backup set.  A backup set consists of a set of file systems
    and a set of rotating backup pools use to backup those file systems.
    """

    def __init__(self, name, sourceFileSystemSpecs, backupPoolConfs):
        "sourceFileSystemSpecs can be ZFS file system names or SourceFileSystemConf objects"
        if not name.isalnum():  # used as a separator in snapshot names
            raise BackupConfigError("backup set name may only contain alpha-numeric characters, got '{}'".format(name))
        self.name = name
        self.sourceFileSystemConfs = self._buildSourceFileSystemConfs(sourceFileSystemSpecs)
        self.backupPoolConfs = tuple(backupPoolConfs)
        self.byBackupPoolName = OrderedDict()
        for backupPoolConf in self.backupPoolConfs:
            self._addBackupPoolConf(backupPoolConf)

    def __str__(self):
        return self.name

    @property
    def backupPoolNames(self):
        return list(self.byBackupPoolName.keys())

    def _buildSourceFileSystemConfs(self, sourceFileSystemSpecs):
        seen = set()
        confs = []
        for fsSpec in sourceFileSystemSpecs:
            fs = self._mkSourceFileSystemConf(fsSpec)  # name will be normalized, need before seen check
            if fs.name in seen:
                raise BackupConfigError("duplicate file system in BackupSetConf: " + fs.name)
            seen.add(fs.name)
            confs.append(fs)
        return tuple(confs)

    def _mkSourceFileSystemConf(self, fsSpec):
        if isinstance(fsSpec, str):
            return SourceFileSystemConf(fsSpec)
        elif isinstance(fsSpec, SourceFileSystemConf):
            return fsSpec
        else:
            raise BackupConfigError("source file system specification is not an instance of SourceFileSystemConf or string: " + str(type(SourceFileSystemConf)))

    def _addBackupPoolConf(self, backupPoolConf):
        if not isinstance(backupPoolConf, BackupPoolConf):
            raise BackupConfigError("back pool is not an instance of BackPoolConf: " + str(type(backupPoolConf)))
        if backupPoolConf.name in self.byBackupPoolName:
            raise BackupConfigError("multiple entries for backupPool " + backupPoolConf.name)
        self.byBackupPoolName[backupPoolConf.name] = backupPoolConf

    def getBackupPoolConf(self, backupPoolName):
        backupPoolConf = self.byBackupPoolName.get(backupPoolName)
        if backupPoolName is None:
            raise BackupConfigError("backup pool {} not part of backup set {}".format(backupPoolName, self.name))
        return backupPoolConf

    def findSourceFileSystem(self, sourceFileSystemName):
        "find source file system, or None"
        for fs in self.sourceFileSystemConfs:
            if fs.name == sourceFileSystemName:
                return fs
        return None

    def getSourceFileSystem(self, sourceFileSystemName):
        "get source file system, or error"
        fs = self.findSourceFileSystem(sourceFileSystemName)
        if fs is None:
            raise BackupConfigError("can't find source file system: {} in BackupSet".format(sourceFileSystemName. self.name))
        return fs

class BackupConf(object):
    "Configuration of backups"
    def __init__(self, backupSets, lockFile="/var/run/zfszipper.lock", recordFilePattern=None,
                 syslogFacility=None, syslogLevel="info", stderrLogging=False):
        """
        lockFile - lock file to use, defaults to /var/run/zfszipper.lock
        recordFilePattern - Pattern used to create TSV record file of backups.  Formatted with strftime with current GMT to make a file path
        syslogFacility - if specified, use log with syslog and log to this facility
        syslogLevel - use this syslog level is syslogFacility is specified, defaults to `info'.
        """
        self.backupSets = backupSets
        self.lockFile = lockFile
        self.recordFile = time.strftime(recordFilePattern, time.gmtime()) if recordFilePattern is not None else None
        self.syslogFacility = loggingOps.parseFacility(syslogFacility) if syslogFacility is not None else None
        self.syslogLevel = loggingOps.parseLevel(syslogLevel)
        self.stderrLogging = stderrLogging

    def getBackupSet(self, backupSetName):
        for backupSet in self.backupSets:
            if backupSet.name == backupSetName:
                return backupSet
        raise BackupSetConf("unknown backup set: " + backupSetName)

    def findSourceFileSystemBackupSets(self, sourceFileSystemName):
        """return list of backupSets containing source file system or empty list"""
        backupSets = []
        for backupSet in self.backupSets:
            fs = backupSets.findSourceFileSystem(sourceFileSystemName)
            if fs is not None:
                backupSets.append(fs)
        return backupSets

    def _listBackupSet(self, backupSet, fh):
        print("backup set:", backupSet.name, file=fh)
        for backupPool in backupSet.byBackupPoolName.values():
            print("\tbackup pool:", backupPool.name, file=fh)
        for sourceFs in backupSet.sourceFileSystemConfs:
            print("\tsource fs:", sourceFs.name, file=fh)

    def listBackupSets(self, fh):
        for backupSet in self.backupSets:
            self._listBackupSet(backupSet, fh)
