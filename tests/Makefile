# -*- mode: makefile-gmake -*-
PYTHON = python3

test: backupLibTests vtest

backupLibTests:
	 ${PYTHON} backupLibTests.py

# slow and requires local ZFS
ltest: zfsLocalSystemTests

zfsLocalSystemTests:
	${PYTHON} zfsLocalSystemTests.py

# virtual disk test must be run as root
vtest:
	 sudo ${PYTHON} virtualDiskTests.py test

vtestclean:
	 sudo ${PYTHON} virtualDiskTests.py clean

clean: vtestclean

