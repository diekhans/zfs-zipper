"""
Mock Zfs object, returns pre-configured values for queries and logs action commands
"""
import sys
from zfszipper.zfs import ZfsPool, ZfsFileSystem, ZfsSnapshot
from collections import OrderedDict
from zfszipper.typeOps import asNameOrStr

def zfsSnapshotNameToFileSystemName(snapshotSpec):
    "convert a zfs snapshot name to a file system name"
    snapshotName = asNameOrStr(snapshotSpec)
    parts = snapshotName.split('@', 1)
    if len(parts) != 2:
        raise Exception("can't parse file system name from snapshot name {}".format(snapshotName))
    return parts[0]

def zfsFileSystemNameToPoolName(fileSystemName):
    "extract the pool name of a zfs file system name"
    if fileSystemName.startswith('/'):
        raise Exception("zfs file system name should not start with '/', count this be mount? {}".format(fileSystemName))
    return fileSystemName.split('/')[0]

def fakeZfsFileSystem(fileSystemName, mounted=True):
    """create fake file system without rest of mock frameworks.  defaults
    parameters for easy fake construction"""
    return ZfsFileSystem(fileSystemName, "/mnt/" + fileSystemName, mounted)

class ZfsMockNode(object):
    """Nodes in a tree of pool -> fs -> snapshot, indexed by name and contain Zfs module objects
    as entries"""
    def __init__(self, entry):
        self.entry = entry   # None if root
        self.children = OrderedDict()  # Nodes, indexed by name

    def addChildNode(self, entry):
        if entry.name in self.children:
            raise Exception("{} entry named {} already exists".format(type(entry), entry.name))
        return self.obtainChildNode(entry)

    def delChildNodeByName(self, name):
        if name not in self.children:
            raise Exception("entry named {} does not exists".format(name))
        del self.children[name]

    def obtainChildNode(self, entry):
        node = self.children.get(entry.name)
        if node is None:
            node = self.children[entry.name] = ZfsMockNode(entry)
        return node

    def findChildNode(self, name):
        return self.children.get(name)

    def getChildNode(self, name):
        return self.children[name]

    def findChildEntry(self, name):
        node = self.children.get(name)
        return None if node is None else node.entry

    def getChildEntry(self, name):
        return self.getChildNode(name).entry

    def getChildEntries(self):
        return [n.entry for n in self.children.values()]

class ZfsMock(object):
    def __init__(self):
        self.root = ZfsMockNode(None)
        self.actions = []

    def add(self, pool, fileSystem=None, snapshotSpecs=()):
        """Add pool, filesystem and snapshots to a ZfsMock, Adding the pool
        and file system may already be in the ZfsMock tree..  snapshotsSpecs
        can be ZfsSnapshot object, simple snapnames, or full snapshotnames"""
        if not isinstance(pool, ZfsPool):
            raise Exception("pool not ZfsPool object: {} {}".format(type(pool), str(pool)))
        poolNode = self.root.obtainChildNode(pool)
        if fileSystem is not None:
            if not isinstance(fileSystem, ZfsFileSystem):
                raise Exception("fileSystem not ZfsFileSystem: {} {}".format(type(fileSystem), str(fileSystem)))
            fsNode = poolNode.obtainChildNode(fileSystem)
            for snapshotSpec in snapshotSpecs:
                self._addSnapshot(fsNode, snapshotSpec)

    def _addSnapshot(self, fsNode, snapshotSpec):
        if isinstance(snapshotSpec, ZfsSnapshot):
            snapshot = snapshotSpec
        else:
            snapshotName = asNameOrStr(snapshotSpec)
            if snapshotSpec.find('@') >= 0:
                snapshot = ZfsSnapshot(snapshotName)
            else:
                snapshot = ZfsSnapshot.factory(fsNode.entry.name, snapshotName)

        if fsNode.findChildNode(snapshot.name) is not None:
            raise Exception("snapshot already exists: {}".format(snapshot.name))
        # make sure these are added in order
        existingSnapShotNames = tuple(fsNode.children.keys())
        if len(existingSnapShotNames) > 0:
            if snapshot.name < existingSnapShotNames[-1]:
                raise Exception("snapshots added out of order {} precedes existing {}".format(snapshot.name, existingSnapShotNames[-1]))
        fsNode.obtainChildNode(snapshot)  # add

    def _recordAction(self, *args):
        self.actions.append(" ".join(args))

    def dump(self, fh=sys.stderr):
        for poolNode in self.root.children.values():
            print("pool:", poolNode.entry.name, file=fh)
            for fsNode in poolNode.children.values():
                print("  filesystem:", fsNode.entry.name, file=fh)
                for snapshot in fsNode.children.values():
                    print("    snapshot:", snapshot.entry.name, file=fh)

    def _notImplemented(self):
        raise Exception("ZfsMock function not implemented, please add if needed")

    def _addFileSystemByName(self, fileSystemName):
        poolNode = self._findPoolNodeByFileSystemName(fileSystemName)
        poolNode.addChildNode(fakeZfsFileSystem(fileSystemName))

    def _addSnapshotByName(self, snapshotSpec):
        fsNode = self._findFileSystemNodeByName(zfsSnapshotNameToFileSystemName(snapshotSpec))
        self._addSnapshot(fsNode, snapshotSpec)

    def _findPoolNodeByFileSystemName(self, fileSystemName):
        poolName = zfsFileSystemNameToPoolName(fileSystemName)
        poolNode = self.root.getChildNode(poolName)
        if poolNode is None:
            return None
        return poolNode

    def _findFileSystemNodeByName(self, fileSystemName):
        "None if not found"
        poolNode = self._findPoolNodeByFileSystemName(fileSystemName)
        if poolNode is None:
            return None
        return poolNode.children.get(fileSystemName)

    def _getFileSystemNodeByName(self, fileSystemName):
        """error if not found"""
        fsNode = self._findFileSystemNodeByName(fileSystemName)
        if fsNode is None:
            raise Exception("file system node not found: {}".format(fileSystemName))
        return fsNode

    def _findFileSystemNodeFromSnapshotName(self, snapshotSpec):
        return self._findFileSystemNodeByName(zfsSnapshotNameToFileSystemName(snapshotSpec))

    def _getFileSystemNodeFromSnapshotName(self, snapshotSpec):
        return self._getFileSystemNodeByName(zfsSnapshotNameToFileSystemName(snapshotSpec))

    def _findSnapshotByName(self, snapshotSpec):
        fsNode = self._findFileSystemNodeFromSnapshotName(snapshotSpec)
        if fsNode is None:
            return None
        return fsNode.findChildEntry(asNameOrStr(snapshotSpec))

    def _getSnapshotByName(self, snapshotSpec):
        snapshot = self._findSnapshotByName(snapshotSpec)
        if snapshot is None:
            raise Exception("snapshot node not found: {}".format(snapshotSpec))
        return snapshot

    def listPools(self):
        return [n.entry for n in self.root.children.values()]

    def findPool(self, poolName):
        node = self.root.getChildNode(poolName)
        return node.entry if node is not None else None

    def listSnapshots(self, fileSystemSpec):
        "parameters can be names or zfs objects"
        fsNode = self._getFileSystemNodeByName(asNameOrStr(fileSystemSpec))
        return fsNode.getChildEntries()

    def listFileSystems(self, poolSpec):
        "parameter can be names or zfs object"
        self._notImplemented()

    def findFileSystem(self, fileSystemName):
        fsNode = self._findFileSystemNodeByName(fileSystemName)
        if fsNode is None:
            return None
        return fsNode.entry

    def getFileSystem(self, fileSystemName):
        "returns a ZfsFileSystem or error"
        fileSystem = self.findFileSystem(fileSystemName)
        if fileSystem is None:
            raise Exception("can't find ZFS file system {}".format(fileSystemName))
        return fileSystem

    def createFileSystem(self, fileSystemName, mounted=True):
        poolNode = self._findPoolNodeByFileSystemName(fileSystemName)
        fsNode = poolNode.addChildNode(fakeZfsFileSystem(fileSystemName, mounted=mounted))
        self._recordAction("zfs", "create", fileSystemName)
        return fsNode.entry

    def createSnapshot(self, snapshotSpec):
        fsNode = self._findFileSystemNodeFromSnapshotName(snapshotSpec)
        fsNode.addChildNode(ZfsSnapshot(snapshotSpec))
        self._recordAction("zfs", "snapshot", snapshotSpec)

    def destroySnapshot(self, snapshotSpec):
        snapshotName = asNameOrStr(snapshotSpec)
        fsNode = self._findFileSystemNodeFromSnapshotName(snapshotName)
        fsNode.delChildNodeByName(snapshotName)
        self._recordAction("zfs", "destroy", "-fp", snapshotName)
        return (("destroy", snapshotName), ("reclaim", "50000"))

    def renameSnapshot(self, oldSnapshotSpec, newSnapshotSpec):
        oldSnapshot = self._getSnapshotByName(oldSnapshotSpec)
        newSnapshot = ZfsSnapshot(asNameOrStr(newSnapshotSpec))
        fsNode = self._findFileSystemNodeFromSnapshotName(oldSnapshot.name)
        fsNode.delChildNodeByName(oldSnapshot.name)
        fsNode.addChildNode(newSnapshot)
        self._recordAction("zfs", "rename", oldSnapshot.name, newSnapshot.name)

    def _recordSendRecv(self, sendCmd, recvCmd):
        cmd = sendCmd + ["|"] + recvCmd
        self._recordAction(*cmd)

    def sendRecvFull(self, sourceSnapshotSpec, backupSnapshotSpec):
        # parse to check if they are valid
        sourceSnapshotName = asNameOrStr(sourceSnapshotSpec)
        backupSnapshotName = asNameOrStr(backupSnapshotSpec)
        ZfsSnapshot(sourceSnapshotName)
        if not self._findSnapshotByName(sourceSnapshotName):
            raise Exception("sendRecvFull source snapshot does not exist: {}", sourceSnapshotName)
        ZfsSnapshot(backupSnapshotName)
        if self._findSnapshotByName(backupSnapshotName):
            raise Exception("sendRecvFull backup snapshot already exists: {}", backupSnapshotName)

        sendCmd = ["zfs", "send", "-P", sourceSnapshotName]
        recvCmd = ["zfs", "receive", '-F']
        backupFsName = zfsSnapshotNameToFileSystemName(backupSnapshotName)
        if self._findFileSystemNodeByName(backupFsName) is None:
            self._addFileSystemByName(backupFsName)
        recvCmd.append(backupSnapshotName)
        self._addSnapshotByName(backupSnapshotName)
        self._recordSendRecv(sendCmd, recvCmd)
        return (("full", sourceSnapshotName, "50000"), ("size", "50000"))

    def sendRecvIncr(self, sourceBaseSnapshotSpec, sourceSnapshotSpec, backupSnapshotSpec):
        # parse to check if they are valid, check that base exists in backup
        sourceBaseSnapshotName = asNameOrStr(sourceBaseSnapshotSpec)
        sourceSnapshotName = asNameOrStr(sourceSnapshotSpec)
        backupSnapshotName = asNameOrStr(backupSnapshotSpec)

        sourceBaseSnapshot = ZfsSnapshot(sourceBaseSnapshotName)
        if not self._findSnapshotByName(sourceBaseSnapshotName):
            raise Exception("sendRecvIncr source base snapshot does not exist: {}", sourceBaseSnapshotName)
        sourceSnapshot = ZfsSnapshot(sourceSnapshotName)
        if not self._findSnapshotByName(sourceSnapshotName):
            raise Exception("sendRecvIncr source snapshot does not exist: {}", sourceSnapshotName)
        backupSnapshot = ZfsSnapshot(backupSnapshotName)
        if self._findSnapshotByName(backupSnapshotName):
            raise Exception("sendRecvIncr backup snapshot already exists: {}", backupSnapshotName)
        backupBaseSnapshot = ZfsSnapshot.factory(backupSnapshot.fileSystem, sourceBaseSnapshot.snapName)
        if not self._findSnapshotByName(backupBaseSnapshot.name):
            raise Exception("sendRecvIncr incremental base send snapshot for {} does not exist in received file system {}".format(backupBaseSnapshot.name, backupSnapshot.fileSystem))
        if sourceSnapshot.snapName <= sourceBaseSnapshot.snapName:
            raise Exception("sendRecvIncr incremental send snapshot {} is earlier than base {}".format(sourceSnapshot.name, sourceBaseSnapshot.name))
        sendCmd = ["zfs", "send", "-P", "-i", sourceBaseSnapshotName, sourceSnapshotName]
        recvCmd = ["zfs", "receive", backupSnapshotName]
        self._addSnapshotByName(backupSnapshotName)
        self._recordSendRecv(sendCmd, recvCmd)
        return (("incremental", sourceBaseSnapshotName, sourceSnapshotName, "50000"), ("size", "50000"))
