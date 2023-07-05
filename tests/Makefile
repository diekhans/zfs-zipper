# -*- mode: makefile-gmake -*-
PYTHON = python3

hostname = $(shell hostname -s)

test:: backupLibTests vtest
ifeq (${hostname}, osprey)
test :: ltest
endif

backupLibTests: backupSnapshotTests backuperTests

backupSnapshotTests:
	 ${PYTHON} backupLibTests.py BackupSnapshotTests

backuperTests:
	 ${PYTHON} backupLibTests.py BackuperTests

# requires local ZFS
ltest: zfsLocalSystemTests

zfsLocalSystemTests:
	${PYTHON} zfsLocalSystemTests.py

# virtual disk test must be run as root
# also seems to want to read from stdin, so redirect
vtest:
	 sudo ${PYTHON} virtualDiskTests.py test </dev/stdin

vtestnoclean:
	 sudo ${PYTHON} virtualDiskTests.py --noClean test </dev/stdin

vtestclean:
	 sudo ${PYTHON} virtualDiskTests.py clean </dev/stdin

clean: vtestclean
	sudo rm -rf __pycache__ output

