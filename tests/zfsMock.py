"""
Mock Zfs object, returns pre-configured values for queries and logs action commands
"""
from zfszipper.zfs import ZfsPool, ZfsFileSystem, ZfsSnapshot
from collections import OrderedDict, defaultdict
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

    def addSendRecvInfo(self, rows):
        "add one set of restuls for Zfs.sendRecv* function "
        self.sendRecvInfo.append(rows)

    def addSendRecvInfos(self, rowsList):
        "add multiple sets of restuls for Zfs.sendRecv* function "
        self.sendRecvInfo.extend(rowsList)

    def __buildPoolLookup(self, poolEntries):
        for poolEntry in poolEntries:
            assert isinstance(poolEntry[0], ZfsPool)
            self.poolsByName[poolEntry[0].name] = poolEntry
            self.__buildFileSystemLookup(poolEntry[0].name, poolEntry[1])
            
    def __buildFileSystemLookup(self, poolName, fileSystemEntries):
        for fileSystemEntry in fileSystemEntries:
            assert isinstance(fileSystemEntry[0], ZfsFileSystem)
            self.fileSystemsByName[fileSystemEntry[0].name] = fileSystemEntry
            for snapshotEntry in fileSystemEntry[1]:
                assert isinstance(snapshotEntry, ZfsSnapshot)

    def dump(self, fh):
        for poolEntry in self.poolsByName.values():
            fh.write("pool:" + str(poolEntry[0]) + "\n")
            for fileSystemEntry in poolEntry[1]:
                fh.write("  filesystem:" + str(fileSystemEntry[0]) + "\n")
                for snapshot in fileSystemEntry[1]:
                    fh.write("    snapshot:" + str(snapshot) + "\n")
                
    def listSnapshots(self, fileSystem):
        return self.fileSystemsByName[asNameOrStr(fileSystem)][1]

    def listFileSystems(self, pool):
        return self.poolsByName[asNameOrStr(pool)][1]

    def getFileSystem(self, pool, fileSystemName):
        entry = self.fileSystemsByName.get(fileSystemName)
        if entry == None:
            return None
        assert(entry[0].poolName == asNameOrStr(pool))
        return entry[0]

    def listPools(self):
        return [poolEntry[0] for poolEntry in self.entries]

    def getPool(self, poolName):
        entry = self.poolsByName.get(poolName)
        return entry[0] if entry != None else None

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
