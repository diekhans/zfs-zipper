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
vtest:
	 sudo ${PYTHON} virtualDiskTests.py test

vtestnoclean:
	 sudo ${PYTHON} virtualDiskTests.py --noClean test

vtestclean:
	 sudo ${PYTHON} virtualDiskTests.py clean

clean: vtestclean
	sudo rm -rf __pycache__ output

