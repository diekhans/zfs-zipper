"""
Support for ZFS interface.  Objects are dynamically constructed from zpool/zfs commands and
not contain the hierarchy.
"""
import re
from collections import namedtuple
from enum import Enum
from .typeOps import asNameOrStr, splitTabLinesToRows
from .cmdrunner import CmdRunner

class Zfs(object):
    "object to handle all calls to ZFS commands."
    def __init__(self):
        self.cmdRunner = CmdRunner()

    def listPools(self):
        "returns list of ZfsPool"
        return [ZfsPool(name, getZfsPoolHealth(health))
                for name, health in self.cmdRunner.callTabSplit(["zpool", "list", "-H", "-o", "name,health"])]

    def _listExportedParsePool(self, poolName, lineIter):
        "parse next pool out of lines from zpool import"
        for line in lineIter:
            m = re.match("^  state: (.*)$", line)
            if m is not None:
                return ZfsPool(poolName, getZfsPoolHealth(m.group(1)))
        raise Exception("zpool export parsing error: `state:' not found")

    def listExported(self):
        "list exported pools available for import"
        exported = []
        lineIter = iter(self.cmdRunner.call(["zpool", "import"]))
        for line in lineIter:
            m = re.match("^   pool: (.*)$", line)
            if m is not None:
                exported.append(self._listExportedParsePool(m.group(1), lineIter))
        return exported

    def havePool(self, poolName):
        "determine if a pool exists"
        for name in self.cmdRunner.call(["zpool", "list", "-H", "-o", "name"]):
            if name == poolName:
                return True
        return False

    def findPool(self, poolName):
        "returns ZfsPool or None"
        if not self.havePool(poolName):
            return None
        else:
            results = self.cmdRunner.callTabSplit(["zpool", "list", "-H", "-o", "name,health", poolName])
            return ZfsPool(results[0][0], getZfsPoolHealth(results[0][1]))

    def listFileSystems(self, poolSpec):
        "returns list of ZfsFileSystem, Pool can be name or object"
        poolName = asNameOrStr(poolSpec)
        return [ZfsFileSystem(row[0], row[1], row[2])
                for row in self.cmdRunner.callTabSplit(["zfs", "list", "-Hr", "-t", "filesystem", "-o", "name,mountpoint,mounted", poolName])]

    def findFileSystem(self, fileSystemName):
        "returns a ZfsFileSystem or None"
        results = self.cmdRunner.callTabSplit(["zfs", "list", "-H", "-t", "filesystem", "-o", "name,mountpoint,mounted"])
        for row in results:
            if row[0] == fileSystemName:
                return ZfsFileSystem(row[0], row[1], row[2])
        return None

    def getFileSystem(self, fileSystemName):
        "returns a ZfsFileSystem or error"
        fileSystem = self.findFileSystem(fileSystemName)
        if fileSystem is None:
            raise Exception("can't find ZFS file system {}".format(fileSystemName))
        return fileSystem

    def createFileSystem(self, fileSystemName):
        "create a new file system"
        self.cmdRunner.call(["zfs", "create", "-p", fileSystemName])
        return self.getFileSystem(fileSystemName)

    def listSnapshots(self, fileSystemSpec):
        "returns list of snapshot names, ordered oldest to newest.  FileSystem can be name or object"
        return [ZfsSnapshot(name)
                for name in self.cmdRunner.call(["zfs", "list", "-Hd", "1", "-t", "snapshot", "-o", "name", "-s", "creation", asNameOrStr(fileSystemSpec)])]

    def importPool(self, poolSpec):
        "import specified pool"
        self.cmdRunner.call(["zpool", "import", asNameOrStr(poolSpec)])

    def exportPool(self, poolSpec):
        "export specified pool"
        self.cmdRunner.call(["zpool", "export", asNameOrStr(poolSpec)])

    def createSnapshot(self, snapshotSpec):
        self.cmdRunner.call(["zfs", "snapshot", asNameOrStr(snapshotSpec)])

    def destroySnapshot(self, snapshotSpec):
        return self.cmdRunner.callTabSplit(["zfs", "destroy", "-fp", asNameOrStr(snapshotSpec)])

    def renameSnapshot(self, oldSnapshotSpec, newSnapshotSpec):
        self.cmdRunner.call(["zfs", "rename", asNameOrStr(oldSnapshotSpec), asNameOrStr(newSnapshotSpec)])

    def sendRecvFull(self, sourceSnapshotSpec, backupSnapshotSpec):
        "return results of send -P parsed into rows of columns"
        sourceSnapshotName = asNameOrStr(sourceSnapshotSpec)
        backupSnapshotName = asNameOrStr(backupSnapshotSpec)

        sendCmd = ["zfs", "send", "-P", sourceSnapshotName]
        recvCmd = ["zfs", "receive", '-F']
        recvCmd.append(backupSnapshotName)
        stderr1, ignored = self.cmdRunner.pipeline2(sendCmd, recvCmd)
        return splitTabLinesToRows(stderr1)

    def sendRecvIncr(self, sourceBaseSnapshotName, sourceSnapshotName, backupSnapshotName):
        "return results of send -P parsed into rows of columns"
        # receive -F is require to prevent "destination X has been modified" error
        sendCmd = ["zfs", "send", "-P", "-i", sourceBaseSnapshotName, sourceSnapshotName]
        recvCmd = ["zfs", "receive", "-F", backupSnapshotName]
        stderr1, ignored = self.cmdRunner.pipeline2(sendCmd, recvCmd)
        return splitTabLinesToRows(stderr1)

    def setProp(self, fileSystemName, name, value):
        "set a property"
        self.cmdRunner.call(["zfs", "set", name + "=" + str(value), fileSystemName])

ZfsPoolHealth = Enum("ZfsPoolHealth", ("ONLINE", "DEGRADED", "FAULTED", "OFFLINE", "REMOVED", "UNAVAIL"))
def getZfsPoolHealth(strVal):
    return getattr(ZfsPoolHealth, strVal)

class ZfsSnapshot(namedtuple("ZfsSnapshot", ("name", "fileSystem", "snapName"))):
    __slots__ = ()

    def __new__(cls, name):
        parts = name.split("@")
        if len(parts) != 2:
            raise Exception("invaliid ZFS snapshot name, should be filesystem@snapName: {}".format(name))
        return super(ZfsSnapshot, cls).__new__(cls, name, parts[0], parts[1])

    @staticmethod
    def factory(fileSystem, snapName):
        return ZfsSnapshot(fileSystem + "@" + snapName)

class ZfsFileSystem(object):
    def __init__(self, name, mountpoint, mounted):
        "mounted can be string yes/no or bool"
        self.name = name
        # empty becomes None:
        self.mountpoint = mountpoint if (mountpoint is None) or len(mountpoint) > 0 else None
        self.mounted = self._parseMounted(mounted)

    def __str__(self):
        return "name=" + self.name + " mountpoint=" + str(self.mountpoint) + " mounted=" + str(self.mounted)

    @staticmethod
    def _parseMounted(mounted):
        if isinstance(mounted, bool):
            return mounted
        elif mounted == 'yes':
            return True
        elif mounted == 'no':
            return False
        else:
            raise ValueError("invalid value for mounted: " + str(mounted))

class ZfsPool(namedtuple("ZfsPool", ("name", "health"))):
    __slots__ = ()
