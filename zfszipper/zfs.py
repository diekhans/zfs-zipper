"""
support for ZFS
"""
from collections import namedtuple
from enum import Enum
import subprocess, tempfile
from .typeops import asNameOrStr, splitTabLinesToRows

class Zfs(object):
    "object to handle all calls to ZFS commands."
    def __init__(self, cmdRunner):
        self.cmdRunner = cmdRunner

    def listSnapshots(self, fileSystem):
        "returns list of snapshot names, ordered oldest to newest.  FileSystem can be name or object"
        return [ZfsSnapshot(name)
                for name in self.cmdRunner.call(["zfs", "list", "-Hd", "1", "-t", "snapshot", "-o", "name", "-s", "creation", asNameOrStr(fileSystem)])]

    def listFileSystems(self, pool):
        "returns list of ZfsFileSystem, Pool can be name or object"
        poolName = asNameOrStr(pool)
        return [ZfsFileSystem(row[0], poolName, row[1], (True if row[2] == "yes" else False))
                for row in self.cmdRunner.callTabSplit(["zfs", "list", "-Hr", "-t", "filesystem", "-o", "name,mountpoint,mounted", poolName])]

    def getFileSystem(self, pool, fileSystemName):
        "returns a ZfsFileSystem or None. Pool can be name or object."
        poolName = asNameOrStr(pool)
        results = self.cmdRunner.callTabSplit(["zfs", "list", "-Hr", "-t", "filesystem", "-o", "name,mountpoint,mounted", poolName, fileSystemName])
        if len(results) == 0:
            return None
        else:
            row = results[0]
            return ZfsFileSystem(row[0], poolName, row[1], (True if row[2] == "yes" else False))

    def listPools(self):
        "returns list of ZfsPool"
        return [ZfsPool(name, getZfsPoolHealth(health))
                for name,health in self.cmdRunner.callTabSplit(["zpool", "list", "-H", "-o", "name,health"])]

    def getPool(self, poolName):
        "returns ZfsPool or None"
        results = self.cmdRunner.call(["zpool", "list", "-H", "-o", "name,health", poolName])
        if len(results) == 0:
            return None
        else:
            return ZfsPool(results[0], getZfsPoolHealth(results[1]))

    def createSnapshot(self, snapshotName):
        self.cmdRunner.run("zfs", "snapshot", snapshotName)
    
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
ZfsFileSystem = namedtuple("ZfsFileSystem", ("name", "poolName", "mountpoint", "mounted"))
ZfsPool = namedtuple("ZfsPool", ("name", "health"))
