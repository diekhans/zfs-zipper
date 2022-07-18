# -*- mode: makefile-gmake -*-
PYTHON = python3
.SECONDARY:
sys = $(shell uname -s)
prefix = /usr/local
sbinDir = ${prefix}/sbin
libDir = ${prefix}/lib
etcDir = ${prefix}/etc

libPyDir = lib/zfs-zipper/zfszipper
libPyFiles = $(wildcard ${libPyDir}/*.py)
libPycDir = ${libPyDir}/__pycache__
sbinProgs = sbin/zfs-zipper sbin/zfs-zipper-diff
etcFiles = etc/zfs-zipper.conf.py
periodicFiles = etc/periodic/daily/100.zfs-zipper

pycCompileDone = ${libPycDir}/.compile.done
pycInstallDone = ${prefix}/${libPycDir}/.install.done


installedFiles = ${libPyFiles:%=${prefix}/%} \
	${libPycFiles:%=${prefix}/%} \
	${sbinProgs:%=${prefix}/%} \
	${etcFiles:%=${prefix}/%} \
	${pycInstallDone}

ifeq (${sys}, FreeBSD)
  installedFiles += ${periodicFiles:%=${prefix}/%}
endif

uninstallDirs = ${libDir}/zfs-zipper

all: ${pycCompileDone}

${pycCompileDone}: ${libPyFiles}
	@mkdir -p $(dir $@)
	PYTHONPATH=lib/zfs-zipper ${PYTHON} -B -c 'import compileall; compileall.compile_dir("${libPyDir}")'
	touch $@

tags: etags
etags:
	etags-emacs --language=python ${libPyFiles} ${sbinProgs}

# must clean tests first, as it runs python code.
clean:
	cd tests && ${MAKE} clean
	rm -rf __pycache__ ${libPycDir} TAGS

flake8: link
lint:
	${PYTHON} -m flake8 ${sbinProgs} lib tests

test:
	cd tests && ${MAKE} test

ltest:
	cd tests && ${MAKE} ltest

vtest:
	cd tests && ${MAKE} vtest

vtestclean:
	cd tests && ${MAKE} vtestclean

####
# install related
####
install: ${installedFiles}

${libDir}/%.py: lib/%.py
	@mkdir -p $(dir $@)
	cp -f $< $@
	chmod a+r,a-wx $@

${pycInstallDone}: ${pycCompileDone}
	@mkdir -p $(dir $@) ${prefix}/${libPycDir}
	cp ${libPycDir}/* ${prefix}/${libPycDir}/
	chmod a+r,a-wx ${prefix}/${libPycDir}/*
	touch $@

${sbinDir}/%: sbin/%
	@mkdir -p $(dir $@)
	rm -f $@.tmp
	echo "#!"`which ${PYTHON}` >$@.tmp
	cat $< >>$@.tmp
	chmod a+rx,a-w $@.tmp
	mv -f $@.tmp $@

${etcDir}/zfs-zipper.conf.py: etc/osprey.zfs-zipper.conf.py
	@mkdir -p $(dir $@)
	cp -f $< $@
	chmod a+r,a-wx $@

${prefix}/etc/periodic/%: etc/periodic/%
	@mkdir -p $(dir $@)
	cp -f $< $@
	chmod a+rx,a-w $@

uninstall:
	rm -f ${installedFiles}
	rm -rf ${uninstallDirs}

