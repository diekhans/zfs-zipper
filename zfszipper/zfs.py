"""
support for ZFS
"""
from collections import namedtuple
from enum import Enum
import subprocess, tempfile
from .typeops import asNameOrStr, splitTabLinesToRows
from .cmdrunner import CmdRunner

class Zfs(object):
    "object to handle all calls to ZFS commands."
    def __init__(self):
        self.cmdRunner = CmdRunner()

    def listSnapshots(self, fileSystem):
        "returns list of snapshot names, ordered oldest to newest.  FileSystem can be name or object"
        return [ZfsSnapshot(name)
                for name in self.cmdRunner.call(["zfs", "list", "-Hd", "1", "-t", "snapshot", "-o", "name", "-s", "creation", asNameOrStr(fileSystem)])]

    def listFileSystems(self, pool):
        "returns list of ZfsFileSystem, Pool can be name or object"
        poolName = asNameOrStr(pool)
        return [ZfsFileSystem(row[0], row[1], row[2])
                for row in self.cmdRunner.callTabSplit(["zfs", "list", "-Hr", "-t", "filesystem", "-o", "name,mountpoint,mounted", poolName])]

    def getFileSystem(self, fileSystemName):
        "returns a ZfsFileSystem or None. Pool can be name or object."
        results = self.cmdRunner.callTabSplit(["zfs", "list", "-H", "-t", "filesystem", "-o", "name,mountpoint,mounted"])
        for row in results:
            if row[0] == fileSystemName:
                return ZfsFileSystem(row[0], row[1], row[2])
        return None

    def listPools(self):
        "returns list of ZfsPool"
        return [ZfsPool(name, getZfsPoolHealth(health))
                for name,health in self.cmdRunner.callTabSplit(["zpool", "list", "-H", "-o", "name,health"])]

    def getPool(self, poolName):
        "returns ZfsPool or None"
        results = self.cmdRunner.callTabSplit(["zpool", "list", "-H", "-o", "name,health", poolName])
        if len(results) == 0:
            return None
        return ZfsPool(results[0][0], getZfsPoolHealth(results[0][1]))

    def createSnapshot(self, snapshotName):
        self.cmdRunner.run(["zfs", "snapshot", snapshotName])
    
    def sendRecvFull(self, sourceSnapshotName, backupSnapshotName, allowOverwrite=False):
        "return results of send -P parsed into rows of columns"
        sendCmd = ["zfs", "send", "-P", sourceSnapshotName]
        recvCmd = ["zfs", "receive"]
        if allowOverwrite:
            recvCmd.append("-F")
        recvCmd.append(backupSnapshotName)
        stderr1, ignored = self.cmdRunner.pipeline2(sendCmd, recvCmd)
        return splitTabLinesToRows(stderr1)
    
    def sendRecvIncr(self, sourceBaseSnapshotName, sourceSnapshotName, backupSnapshotName):
        "return results of send -P parsed into rows of columns"
        sendCmd = ["zfs", "send", "-P", "-i", sourceBaseSnapshotName, sourceSnapshotName]
        recvCmd = ["zfs", "receive", backupSnapshotName]
        stderr1, ignored = self.cmdRunner.pipeline2(sendCmd, recvCmd)
        return splitTabLinesToRows(stderr1)

ZfsPoolHealth = Enum("ZfsPoolHealth", ("ONLINE", "DEGRADED", "FAULTED", "OFFLINE", "REMOVED", "UNAVAIL"))
def getZfsPoolHealth(strVal):
    return getattr(ZfsPoolHealth, strVal)

ZfsSnapshot = namedtuple("ZfsSnapshot", ("name",))
class ZfsFileSystem(object):
    def __init__(self, name, mountpoint, mounted):
        "mounted can be string yes/no or bool"
        self.name = name
        # empty becomes None:
        self.mountpoint = mountpoint if (mountpoint == None) or len(mountpoint) > 0 else None
        self.mounted = self.__parseMounted(mounted)

    @staticmethod
    def __parseMounted(mounted):
        if isinstance(mounted, bool):
            return mounted
        elif mounted == 'yes':
            return True
        elif mounted == 'no':
            return False
        else:
            raise ValueError("invalid value for mounted: " + str(mounted))
        
ZfsPool = namedtuple("ZfsPool", ("name", "health"))
