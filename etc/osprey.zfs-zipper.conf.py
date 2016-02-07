# ZFS-Zipper configuration file.  Edit in source tree, not installed copy
from zfszipper.config import *


# backup pool disks are zfs-mirrored and compressed
#   zpool create -O atime=off -O compression=lz4 -O dedup=on -m /media/zackup1 osprey_zackup1a ada0 ada1
ospreyZackup1aPool = BackupPoolConf("osprey_zackup1a")  # rotation a
ospreyZackup1bPool = BackupPoolConf("osprey_zackup1b")  # rotation b

# osprey zroot, excluding tmp, ports and src
osprey1Set = BackupSetConf("osprey1",
                           [SourceFileSystemConf("a_pool/markd_a"),
                            SourceFileSystemConf("a_pool/osprey"),
                            SourceFileSystemConf("a_pool/photo_a"),
                            SourceFileSystemConf("b_pool/photo_b"),],
                           [ospreyZackup1aPool, ospreyZackup1bPool])
config = BackupConf([osprey1Set],
                    lockFile="/var/run/zfszipper.lock",
                    recordFilePattern="/var/db/zfszipper/%Y/%Y-%m.record.tsv",
                    syslogFacility="local0")

