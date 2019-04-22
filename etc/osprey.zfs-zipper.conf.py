# ZFS-Zipper configuration file.  Edit in source tree, not installed copy
from zfszipper.config import BackupSetConf, SourceFileSystemConf, BackupPoolConf, BackupConf


# backup pool disks are zfs-mirrored and compressed
#   zpool create -O atime=off -O compression=lz4 -O dedup=off -m /media/zackup1 osprey_zackup1a mirror /dev/disk0 /dev/disk1

# two rotations
osprey1Set = BackupSetConf("osprey1",
                           [SourceFileSystemConf("b_pool/photo_b")],
                           [BackupPoolConf("osprey_zackup1a"),
                            BackupPoolConf("osprey_zackup1b")])
osprey2Set = BackupSetConf("osprey2",
                           [SourceFileSystemConf("a_pool/markd_a"),
                            SourceFileSystemConf("a_pool/osprey"),
                            SourceFileSystemConf("a_pool/photo_a")],
                           [BackupPoolConf("osprey_zackup2a"),
                            BackupPoolConf("osprey_zackup2b")])
config = BackupConf([osprey1Set, osprey2Set],
                    lockFile="/var/run/zfszipper.lock",
                    recordFilePattern="/var/db/zfszipper/%Y/%Y-%m.record.tsv",
                    syslogFacility="local0")
