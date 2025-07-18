"""
Code to parse and represent snapshots
"""
import os.path as osp
import time
import re
import logging
from .typeOps import asNameStrOrNone, currentGmtTimeStr
from collections import namedtuple
logger = logging.getLogger()

class BackupSnapshot(namedtuple("BackupSnapshot",
                                ("fileSystemName", "timestamp",
                                 "backupsetName", "oldSuffix"))):
    """Parsed backup snapshot name.  Use create methods, not constructor.
    The oldSuffix is an old style _incr or _full and are no longer created,
    but still parsed.  Note, create functions create these BackupSnapshot
    objects, but not the actually snapshot.

    If zfs terminology the name of the snapshot includes the file system
    the name without it is the snapname, which is our backupsetName.
    """
    # FIXME: terminology is still confusing with much of the code used
    # snapshotName rather than backupsetName
    __slots__ = ()
    # backup set names may name contain `_',
    nameForm = "zipper_<GMT>_<backupset>"
    oldFullForm = "zipper_<GMT>_<backupset>_full"
    oldIncrForm = "zipper_<GMT>_<backupset>_incr"
    # re group:             1       2        3

    prefix = "zipper_"
    gmt = "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    snapshotNameRe = re.compile("^{prefix}({gmt})_([^.]+)(_incr|_full)?$".format(prefix=prefix, gmt=gmt))

    @property
    def name(self):
        "this makes in this behave like the zfs wrapper objects"
        return self.getSnapshotName()

    def getSnapName(self):
        "construct name without FS"
        name = self.prefix + self.timestamp + "_" + self.backupsetName
        if self.oldSuffix is not None:
            name += "_" + self.oldSuffix
        return name

    def getSnapshotName(self):
        "construct name with FS"
        if self.fileSystemName is None:
            return self.getSnapName()
        else:
            return self.fileSystemName + "@" + self.getSnapName()

    @classmethod
    def createFromSnapshotName(cls, snapshotNameSpec, dropFileSystem=False, requireFileSystem=False):
        "Keep file systems name, unless drop specified"
        fileSystemName, snapshotName = cls.splitZfsSnapshotName(snapshotNameSpec)
        timestamp, backupsetName, oldSuffix = cls._parseSnapshotName(snapshotName)
        if requireFileSystem and (fileSystemName is None):
            raise Exception("file system name requred in snapshopt: {}".format(snapshotNameSpec))
        if dropFileSystem:
            fileSystemName = None
        if fileSystemName is not None:
            fileSystemName = osp.normpath(fileSystemName)
        return cls(fileSystemName=fileSystemName, timestamp=timestamp, backupsetName=backupsetName, oldSuffix=oldSuffix)

    def createFromSnapshot(self, fileSystem=None):
        "create from another snapshot, excluding file system, but possible setting a new one"
        return BackupSnapshot(fileSystemName=asNameStrOrNone(fileSystem),
                              timestamp=self.timestamp, backupsetName=self.backupsetName, oldSuffix=self.oldSuffix)

    @classmethod
    def createCurrent(cls, backupsetName, fileSystem=None):
        "create using current timestamp."
        # Ouch, had the problem that the test cases ran so quickly that
        # the one second resoltion of the time can result in snapname
        # collision. To address this, just sleep to force them to be unique.
        time.sleep(2)
        return cls(timestamp=currentGmtTimeStr(), backupsetName=backupsetName, oldSuffix=None,
                   fileSystemName=asNameStrOrNone(fileSystem))

    def __str__(self):
        return self.getSnapshotName()

    @classmethod
    def splitZfsSnapshotName(cls, snapshotName):
        "parse into (filesys, name)"
        parts = snapshotName.split('@')
        if len(parts) > 2:
            raise ValueError("invalid snapshotName '{}', expected zero or one `@'".format(snapshotName))
        if len(parts) == 2:
            return parts
        else:
            return (None, parts[0])

    @classmethod
    def _parseSnapshotName(cls, name):
        "parse a simple snapshot name into (timestr, backupsetName, type)"
        if not name.startswith(BackupSnapshot.prefix):
            raise ValueError("snapshot name doesn't start with '{}', got '{}'".format(BackupSnapshot.prefix, name))
        parse = cls.snapshotNameRe.match(name)
        if parse is None:
            raise ValueError("expected snapshot name in the form '{}', '{}' or '{}, got '{}'".format(cls.nameForm, cls.oldFullForm, cls.oldIncrForm, name))
        timestr = parse.group(1)
        backupSetName = parse.group(2)
        oldSuffix = parse.group(3)
        return (timestr, backupSetName, oldSuffix)

    @classmethod
    def isZipperSnapshot(cls, snapshotName):
        "is this one of ours? Maybe or my not include ZFS fs prefix"
        iBase = snapshotName.find('@') + 1  # start of snapshot name, 0 if no fs in name
        return snapshotName.startswith(cls.prefix, iBase)

def asSnapshotName(snapshotSpec):
    """spec can get be a name of a BackupSnapshot, return the name (less FS)"""
    return snapshotSpec if isinstance(snapshotSpec, str) else snapshotSpec.getSnapName()

class BackupSnapshots(list):
    "list of snapshots objects from a file system, ordered from newest to oldest by default"
    def __init__(self, zfs, fileSystem, *, reverse=True):
        for zfsSnapshot in zfs.listSnapshots(fileSystem.name):
            self._loadSnapshot(zfs, zfsSnapshot)
        self.sort(key=lambda s: s.timestamp, reverse=reverse)

    def _loadSnapshot(self, zfs, zfsSnapshot):
        if BackupSnapshot.isZipperSnapshot(zfsSnapshot.name):
            snapshot = BackupSnapshot.createFromSnapshotName(zfsSnapshot.name)
            self.append(snapshot)

    def findNewestCommon(self, otherSnapshots):
        "return newest command snapshot in self that is also in otherSnapshots"
        for snapshot in self:
            if otherSnapshots.find(snapshot.getSnapName()) is not None:
                return snapshot
        return None

    def findIdx(self, snapshotSpec):
        snapshotName = asSnapshotName(snapshotSpec)
        for idx in range(len(self)):
            if self[idx].getSnapName() == snapshotName:
                return idx
        return -1

    def getIdx(self, snapshotSpec):
        idx = self.findIdx(snapshotSpec)
        if idx < 0:
            raise Exception("Required snapshot not found in list of snapshots: {}".format(asSnapshotName(snapshotSpec)))
        return idx

    def find(self, snapshotSpec):
        idx = self.findIdx(snapshotSpec)
        return self[idx] if idx >= 0 else None

    def get(self, snapshotSpec):
        return self[self.getIdx(snapshotSpec)]
