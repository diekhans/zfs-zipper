# ZFS-Zipper configuration file.  Edit in source tree, not installed copy
from zfszipper.config import BackupSetConf, SourceFileSystemConf, BackupPoolConf, BackupConf


# backup pool disks are zfs-mirrored and compressed
#   zpool create -O atime=off -O compression=lz4 -O dedup=off -m /media/zackup1 osprey_zackup1a mirror /dev/disk0 /dev/disk1

# pools are names in the form osprey_zackup{set}{rotation}
# where set corresponds to a backupset and rotation is a rotation of that set,
# (a, b, c)

# two rotations
osprey1Set = BackupSetConf("osprey1",
                           [SourceFileSystemConf("kettle/photo_b")],
                           [BackupPoolConf("osprey_zackup1a"),
                            BackupPoolConf("osprey_zackup1b"),
                            BackupPoolConf("osprey_zackup1c")])
osprey2Set = BackupSetConf("osprey2",
                           [SourceFileSystemConf("kettle/markd_a"),
                            SourceFileSystemConf("kettle/photo_a"),
                            SourceFileSystemConf("kettle/music-library"),],
                           [BackupPoolConf("osprey_zackup2a"),
                            BackupPoolConf("osprey_zackup2b"),
                            BackupPoolConf("osprey_zackup2c")])
config = BackupConf([osprey1Set, osprey2Set],
                    lockFile="/var/run/zfszipper.lock",
                    recordFilePattern="/var/db/zfszipper/%Y/%Y-%m.record.tsv",
                    syslogFacility="local0")
