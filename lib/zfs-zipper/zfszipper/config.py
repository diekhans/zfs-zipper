"""
Configuration objects.
"""
import os
import time
from collections import OrderedDict
from zfszipper import loggingops

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

    def determineBackupFileSystemName(self, fileSystem):
        """determine ZFS fileSystemName used to backup fileSystem (file systems can be name or zfs.FileSystem)"""
        return os.path.normpath(self.name + "/" + (fileSystem if isinstance(fileSystem, str) else fileSystem.name))

class BackupSetConf(object):
    """Configuration of a backup set.  A backup set consists of a set of file systems
    and a set of rotating backup pools use to backup those file systems.
    """
    def __init__(self, name, sourceFileSystemSpecs, backupPoolConfs):
        "sourceFileSystemSpecs can be ZFS file system names or SourceFileSystemConf objects"
        if name.find('_') >= 0:  # used as a separator in snapshot names
            raise BackupConfigError("backup set name may not contain `_': " + name)
        self.name = name
        self.sourceFileSystemConfs = self.__buildSourceFileSystemConfs(sourceFileSystemSpecs)
        self.backupPoolConfs = tuple(backupPoolConfs)
        self.byBackupPoolName = OrderedDict()
        for backupPoolConf in self.backupPoolConfs:
            self.__addBackupPoolConf(backupPoolConf)

    @property
    def backupPoolNames(self):
        return list(self.byBackupPoolName.iterkeys())

    def __buildSourceFileSystemConfs(self, sourceFileSystemSpecs):
        seen = set()
        confs = []
        for fsSpec in sourceFileSystemSpecs:
            fs = self.__mkSourceFileSystemConf(fsSpec)  # name will be normalized, need before seen check
            if fs.name in seen:
                raise BackupConfigError("duplicate file system in BackupSetConf: " + fs.name)
            seen.add(fs.name)
            confs.append(fs)
        return tuple(confs)

    def __mkSourceFileSystemConf(self, fsSpec):
        if isinstance(fsSpec, str):
            return SourceFileSystemConf(fsSpec)
        elif isinstance(fsSpec, SourceFileSystemConf):
            return fsSpec
        else:
            raise BackupConfigError("source file system specification is not an instance of SourceFileSystemConf or string: " + str(type(SourceFileSystemConf)))

    def __addBackupPoolConf(self, backupPoolConf):
        if not isinstance(backupPoolConf, BackupPoolConf):
            raise BackupConfigError("back pool is not an instance of BackPoolConf: " + str(type(backupPoolConf)))
        if backupPoolConf.name in self.byBackupPoolName:
            raise BackupConfigError("multiple entries for backupPool " + backupPoolConf.name)
        self.byBackupPoolName[backupPoolConf.name] = backupPoolConf

    def getBackupPoolConf(self, backupPoolName):
        backupPoolConf = self.byBackupPoolName.get(backupPoolName)
        if backupPoolName is None:
            raise BackupConfigError("backup pool %s not part of backup set %s" % (backupPoolName, self.name))
        return backupPoolConf

    def getSourceFileSystem(self, sourceFileSystemName):
        "find source file system"

class BackupConf(object):
    "Configuration of backups"
    def __init__(self, backupSets, lockFile="/var/run/zfszipper.lock", recordFilePattern=None,
                 syslogFacility=None, syslogLevel="info"):
        """
        lockFile - lock file to use, defaults to /var/run/zfszipper.lock
        recordFilePattern - Pattern used to create TSV record file of backups.  Formatted with strftime with current GMT to make a file path
        syslogFacility - if specified, use log with syslog and log to this facility
        syslogLevel - use this syslog level is syslogFacility is specified, defaults to `info'.
        """
        self.backupSets = backupSets
        self.lockFile = lockFile
        self.recordFile = time.strftime(recordFilePattern, time.gmtime()) if recordFilePattern is not None else None
        self.syslogFacility = loggingops.parseFacility(syslogFacility) if syslogFacility is not None else None
        self.syslogLevel = loggingops.parseLevel(syslogLevel)

    def getBackupSet(self, backupSetName):
        for backupSet in self.backupSets:
            if backupSet.name == backupSetName:
                return backupSet
        raise BackupSetConf("unknown backup set: " + backupSetName)
