"""
Test of basic ZFS query functions that will only work run on a system with
multiple ZFS file systems and snapshot.  This may not run everywhere.
It does not modify the system or require root.
"""

import sys
import unittest
sys.path.insert(0, "../lib/zfs-zipper")
from zfszipper import zfs

debug = False

class ZfsLocalSystemTest(unittest.TestCase):
    def assertStrCountMin(self, dump, substr, minCount):
        cnt = dump.count(substr)
        if cnt < minCount:
            msg = "ZFS dump didn't have at least {} occurrences of >{}<\n".format(minCount, substr)
            sys.stderr.write(msg + "\n")
            sys.stderr.write(dump)
            self.fail(msg)

    def _zfsLoadFileSystem(self, zfs, fileSystem):
        descs = []
        descs.append("  filesystem: " + str(fileSystem))
        for snapshot in zfs.listSnapshots(fileSystem.name):
            descs.append("    snapshot: " + " ".join(snapshot))
        return descs

    def _zfsLoadPool(self, zfs, pool):
        descs = []
        descs.append("pool: " + str(pool))
        for fileSystem in zfs.listFileSystems(pool.name):
            descs.extend(self._zfsLoadFileSystem(zfs, fileSystem))
        return descs

    def _zfsLoad(self, zfs):
        descs = []
        for pool in zfs.listPools():
            descs.extend(self._zfsLoadPool(zfs, pool))
        return "\n".join(descs) + "\n"

    def testPoolsLoad(self):
        descs = self._zfsLoad(zfs.Zfs())
        if debug:
            print(descs, file=sys.stderr)
        self.assertStrCountMin(descs, "pool", 1)  # kettle
        self.assertStrCountMin(descs, "filesystem", 5)
        self.assertStrCountMin(descs, "snapshot", 10)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ZfsLocalSystemTest))
    return suite

if __name__ == '__main__':
    unittest.main()
