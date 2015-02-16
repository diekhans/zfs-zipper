PYTHON = python

libPyBaseFiles = __init__.py zfs.py config.py cmdrunner.py backup.py loggingops.py typeops.py
libPyRelFiles = ${libPyBaseFiles:%=zfs-zipper/zfszipper/%}
libPycRelFiles = ${libPyRelFiles:%=%c}
libPycSrcFiles = ${libPycRelFiles:%=lib/%}
sbinProgs = zfs-zipper
etcFiles = zfs-zipper.conf.py
periodicFiles=daily/100.zfs-zipper

prefix=/opt
libDir = ${prefix}/lib
sbinDir = ${prefix}/sbin
etcDir = ${prefix}/etc
periodicDir = /usr/local/etc/periodic

installedFiles = ${libPyRelFiles:%=${libDir}/%} ${libPycRelFiles:%=${libDir}/%} ${sbinProgs:%=${sbinDir}/%} ${etcFiles:%=${etcDir}/%} ${periodicFiles:%=${periodicDir}/%}

uninstallDirs = ${libDir}/zfs-zipper/zfszipper ${libDir}/zfs-zipper

all: ${libPycSrcFiles}

install: ${installedFiles}

${libDir}/%.py: lib/%.py
	@mkdir -p $(dir $@)
	cp -f $< $@
	chmod a+r,a-wx $@

# make sure pyc is newer than py
${libDir}/%.pyc: lib/%.pyc ${libDir}/%.py
	@mkdir -p $(dir $@)
	cp -f $< $@
	chmod a+r,a-wx $@

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

${periodicDir}/%: etc/periodic/%
	@mkdir -p $(dir $@)
	cp -f $< $@
	chmod a+rx,a-w $@


%.pyc: %.py
	PYTHONPATH=lib/zfs-zipper ${PYTHON} -B -c 'import compileall;compileall.compile_file("$<")'

clean:
	rm -f ${libPycSrcFiles}

uninstall:
	rm -f ${installedFiles}
	rmdir ${uninstallDirs}
