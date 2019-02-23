"""
Mock Zfs object, returns pre-configured values for queries and logs action commands
"""
import os
from zfszipper.zfs import ZfsPool, ZfsFileSystem, ZfsSnapshot
from collections import OrderedDict
from zfszipper.typeops import asNameOrStr

class ZfsMock(object):
    def __init__(self, poolEntries):
        "poolEntries are hierarchy tuples of zfs objects ((pool1 ((filesystem1 (snapshot1, snapshot2)) ..)))"
        self.poolEntries = poolEntries
        self.poolsByName = OrderedDict()  # map to entry tuples
        self.fileSystemsByName = OrderedDict()
        self.actions = []
        self.sendRecvInfo = []
        self.__buildPoolLookup(poolEntries)

    @staticmethod
    def makeZfsFileSystem(fileSystemName, pool, mounted=True):
        "defaults parameters for easy fake construction"
        return ZfsFileSystem(fileSystemName, "/mnt/" + fileSystemName, mounted)

    def addSendRecvInfo(self, rows):
        "add one set of restuls for Zfs.sendRecv* function "
        self.sendRecvInfo.append(rows)

    def addSendRecvInfos(self, rowsList):
        "add multiple sets of restuls for Zfs.sendRecv* function "
        self.sendRecvInfo.extend(rowsList)

    def __buildPoolLookup(self, poolEntries):
        for poolEntry in poolEntries:
            if not isinstance(poolEntry[0], ZfsPool):
                raise Exception("poolEntry not ZfsPool: {} {}".format(type(poolEntry[0]), str(poolEntry[0])))
            self.poolsByName[poolEntry[0].name] = poolEntry
            self.__buildFileSystemLookup(poolEntry[0].name, poolEntry[1])

    def __buildFileSystemLookup(self, poolName, fileSystemEntries):
        for fileSystemEntry in fileSystemEntries:
            if not isinstance(fileSystemEntry[0], ZfsFileSystem):
                raise Exception("fileSystemEntry not ZfsFileSystem: {} {}".format(type(fileSystemEntry), str(fileSystemEntry)))
            self.fileSystemsByName[fileSystemEntry[0].name] = fileSystemEntry
            for snapshotEntry in fileSystemEntry[1]:
                if not isinstance(snapshotEntry, ZfsSnapshot):
                    raise Exception("snapshotEntry not ZfsSnapshot: {} {}".format(type(snapshotEntry), str(snapshotEntry)))

    def dump(self, fh):
        for poolEntry in self.poolsByName.values():
            fh.write("pool:" + str(poolEntry[0]) + "\n")
            for fileSystemEntry in poolEntry[1]:
                fh.write("  filesystem:" + str(fileSystemEntry[0]) + "\n")
                for snapshot in fileSystemEntry[1]:
                    fh.write("    snapshot:" + str(snapshot) + "\n")

    def listPools(self):
        return [poolEntry[0] for poolEntry in self.entries]

    def getPool(self, poolName):
        entry = self.poolsByName.get(poolName)
        return entry[0] if entry is not None else None

    def listSnapshots(self, fileSystem):
        return self.fileSystemsByName[asNameOrStr(fileSystem)][1]

    def listFileSystems(self, pool):
        return self.poolsByName[asNameOrStr(pool)][1]

    def getFileSystem(self, fileSystemName):
        entry = self.fileSystemsByName.get(fileSystemName)
        if entry is None:
            return None
        return entry[0]

    def __fileSystemNameToPoolName(self, fileSystemName):
        # get top directory
        poolName = os.path.dirname(fileSystemName)
        while True:
            nextDir = os.path.dirname(poolName)
            if nextDir == "":
                return poolName
            poolName = nextDir

    def createFileSystem(self, fileSystemName):
        return self.makeZfsFileSystem(fileSystemName, self.__fileSystemNameToPoolName(fileSystemName))

    def createSnapshot(self, snapshotName):
        self.actions.append(" ".join(("zfs", "snapshot", snapshotName)))

    def __recordSendRecv(self, sendCmd, recvCmd):
        self.actions.append(" ".join(sendCmd) + " | " + " ".join(recvCmd))

    def __popSendRecvInfo(self):
        if len(self.sendRecvInfo) > 0:
            return self.sendRecvInfo.pop()
        else:
            return tuple()

    def sendRecvFull(self, sourceSnapshotName, backupSnapshotName, allowOverwrite=False):
        sendCmd = ["zfs", "send", "-P", sourceSnapshotName]
        recvCmd = ["zfs", "receive"]
        if allowOverwrite:
            recvCmd.append("-F")
        recvCmd.append(backupSnapshotName)
        self.__recordSendRecv(sendCmd, recvCmd)
        return self.__popSendRecvInfo()

    def sendRecvIncr(self, sourceBaseSnapshotName, sourceSnapshotName, backupSnapshotName):
        sendCmd = ["zfs", "send", "-P", "-i", sourceBaseSnapshotName, sourceSnapshotName]
        recvCmd = ["zfs", "receive", backupSnapshotName]
        self.__recordSendRecv(sendCmd, recvCmd)
        return self.__popSendRecvInfo()
