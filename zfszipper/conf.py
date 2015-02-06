"""
Configuration objects.
"""
import os
from collections import OrderedDict

class SourcePoolConf(object):
    "a poll of file systems to backup"
    def __init__(self, name):
        self.name = name

class SourceFileSystemConf(object):
    "a file system to backup"
    def __init__(self, pool, name):
        self.pool = pool
        self.name = name

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
    def __init__(self, name, sourceFileSystems, backupPools):
        self.name = name
        self.sourceFileSystems = tuple(sourceFileSystems)
        self.bySourceFileSystemName = OrderedDict()
        for sourceFileSystem in sourceFileSystems:
            self.__addSourceFileSystem(sourceFileSystem)

        self.backupPools = tuple(backupPools)
        self.byBackupPoolName = OrderedDict()
        for backupPool in self.backupPools:
            self.__addBackupPool(backupPool)

    def __addSourceFileSystem(self, sourceFileSystem):
        if not isinstance(sourceFileSystem, SourceFileSystemConf):
            raise Exception("source file system is not an instance of SourceFileSystemConf: " + str(type(sourceFileSystem)))
        if sourceFileSystem.name in self.bySourceFileSystemName:
            raise Exception("multiple entries for source file system " + sourceFileSystem.name)
        self.bySourceFileSystemName[sourceFileSystem.name] = sourceFileSystem

    def __addBackupPool(self, backupPool):
        if not isinstance(backupPool, BackupPoolConf):
            raise Exception("back pool is not an instance of BackPoolConf: " + str(type(backupPool)))
        if backupPool.name in self.byBackupPoolName:
            raise Exception("multiple entries for backupPool " + backupPool.name)
        self.byBackupPoolName[backupPool.name] = backupPool

    def getSourceFileSystem(self, sourceFileSystemName):
        return self.bySourceFileSystemName.get(sourceFileSystemName)

    def getBackupPool(self, backupPoolName):
        return self.byBackupPoolName.get(backupPoolName)

    
class BackupConf(object):
    "Configuration of backups"
    def __init__(self,  backupSets):
        self.backupSets = backupSets



