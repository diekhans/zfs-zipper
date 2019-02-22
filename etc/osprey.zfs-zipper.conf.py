# ZFS-Zipper configuration file.  Edit in source tree, not installed copy
from zfszipper.config import BackupSetConf, SourceFileSystemConf, BackupPoolConf, BackupConf


# backup pool disks are zfs-mirrored and compressed
#   zpool create -O atime=off -O compression=lz4 -O dedup=on -m /media/zackup1 osprey_zackup1a ada0 ada1

# three rotations
osprey1Set = BackupSetConf("osprey1",
                           [SourceFileSystemConf("a_pool/markd_a"),
                            SourceFileSystemConf("a_pool/osprey"),
                            SourceFileSystemConf("a_pool/photo_a"),
                            SourceFileSystemConf("b_pool/photo_b")],
                           [BackupPoolConf("osprey_zackup1a"),
                            BackupPoolConf("osprey_zackup1b"),
                            BackupPoolConf("osprey_zackup1c")])
config = BackupConf([osprey1Set],
                    lockFile="/var/run/zfszipper.lock",
                    recordFilePattern="/var/db/zfszipper/%Y/%Y-%m.record.tsv",
                    syslogFacility="local0")
