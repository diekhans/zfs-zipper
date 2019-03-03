"""
Mock Zfs object, returns pre-configured values for queries and logs action commands
"""
import sys
from zfszipper.zfs import ZfsPool, ZfsFileSystem, ZfsSnapshot
from collections import OrderedDict
from zfszipper.typeOps import asNameOrStr

def zfsSnapshotNameToFileSystemName(snapshotName):
    "convert a zfs snapshot name to a file system name"
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
        if isinstance(snapshotSpec, str):
            if snapshotSpec.find('@') >= 0:
                snapshot = ZfsSnapshot(snapshotSpec)
            else:
                snapshot = ZfsSnapshot.factory(fsNode.entry.name, snapshotSpec)
        elif not isinstance(snapshotSpec, ZfsSnapshot):
            raise Exception("snapshot not ZfsSnapshot: {} {}".format(type(snapshot), str(snapshot)))
        else:
            snapshot = snapshotSpec
        if fsNode.findChildNode(snapshot.name) is not None:
            raise Exception("snapshot already exists: {}".format(snapshot.name))
        # make sure these are added in order
        existingSnapShotNames = tuple(fsNode.children.keys())
        if len(existingSnapShotNames) > 0:
            if snapshot.name < existingSnapShotNames[-1]:
                raise Exception("snapshots added out of order {} precedes existing {}".format(snapshot.name, existingSnapShotNames[-1]))
        fsNode.obtainChildNode(snapshot)  # add

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

    def _addSnapshotByName(self, snapshotName):
        fsNode = self._findFileSystemNodeByName(zfsSnapshotNameToFileSystemName(snapshotName))
        self._addSnapshot(fsNode, snapshotName)

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

    def _findFileSystemNodeFromSnapshotName(self, snapshotName):
        return self._findFileSystemNodeByName(zfsSnapshotNameToFileSystemName(snapshotName))

    def _findSnapshotByName(self, snapshotName):
        fsNode = self._findFileSystemNodeFromSnapshotName(snapshotName)
        if fsNode is None:
            return None
        return fsNode.findChildEntry(snapshotName)

    def listPools(self):
        return [n.entry for n in self.root.children.values()]

    def findPool(self, poolName):
        node = self.root.getChildNode(poolName)
        return node.entry if node is not None else None

    def listSnapshots(self, fileSystemSpec):
        "parameters can be names or zfs objects"
        fsNode = self._findFileSystemNodeByName(asNameOrStr(fileSystemSpec))
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
        poolNode = self._fileSystemNameToPoolNode(fileSystemName)
        fsNode = poolNode.addChildNode(fakeZfsFileSystem(fileSystemName, mounted=mounted))
        self.actions.append(("zfs", "create", fileSystemName))
        return fsNode.entry

    def createSnapshot(self, snapshotName):
        fsNode = self._findFileSystemNodeFromSnapshotName(snapshotName)
        fsNode.addChildNode(ZfsSnapshot(snapshotName))
        self.actions.append(" ".join(("zfs", "snapshot", snapshotName)))

    def _recordSendRecv(self, sendCmd, recvCmd):
        self.actions.append(" ".join(sendCmd) + " | " + " ".join(recvCmd))

    def sendRecvFull(self, sourceSnapshotName, backupSnapshotName, allowOverwrite=False):
        # parse to check if they are valid
        ZfsSnapshot(sourceSnapshotName)
        if not self._findSnapshotByName(sourceSnapshotName):
            raise Exception("sendRecvFull source snapshot does not exist: {}", sourceSnapshotName)
        ZfsSnapshot(backupSnapshotName)
        if self._findSnapshotByName(backupSnapshotName):
            raise Exception("sendRecvFull backup snapshot already exists: {}", backupSnapshotName)

        sendCmd = ["zfs", "send", "-P", sourceSnapshotName]
        recvCmd = ["zfs", "receive"]
        if allowOverwrite:
            recvCmd.append("-F")  # FIXME: this is not right, see to-do.org
        backupFsName = zfsSnapshotNameToFileSystemName(backupSnapshotName)
        if self._findFileSystemNodeByName(backupFsName) is None:
            self._addFileSystemByName(backupFsName)
        recvCmd.append(backupSnapshotName)
        self._addSnapshotByName(backupSnapshotName)
        self._recordSendRecv(sendCmd, recvCmd)
        return (("full", sourceSnapshotName, "50000"), ("size", "50000"))

    def sendRecvIncr(self, sourceBaseSnapshotName, sourceSnapshotName, backupSnapshotName):
        # parse to check if they are valid, check that base exists in backup
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
            raise Exception("sendRecvIncr incremental base send snapshot for {} does not exist in received file system {}".format(backupBaseSnapshot.name, backupSnapshot.fileSystemName))
        if sourceSnapshot.snapName <= sourceBaseSnapshot.snapName:
            raise Exception("sendRecvIncr incremental send snapshot {} is earlier than base {}".format(sourceSnapshot.name, sourceBaseSnapshot.name))
        sendCmd = ["zfs", "send", "-P", "-i", sourceBaseSnapshotName, sourceSnapshotName]
        recvCmd = ["zfs", "receive", backupSnapshotName]
        self._addSnapshotByName(backupSnapshotName)
        self._recordSendRecv(sendCmd, recvCmd)
        return (("incremental", sourceBaseSnapshotName, sourceSnapshotName, "50000"), ("size", "50000"))
