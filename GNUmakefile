PYTHON = python

libPyBaseFiles = __init__.py zfs.py config.py cmdrunner.py backup.py loggingops.py typeops.py
libPyRelFiles = ${libPyBaseFiles:%=zfs-zipper/zfszipper/%}
libPycRelFiles = ${libPyRelFiles:%=%c}
libPycSrcFiles = ${libPycRelFiles:%=lib/%}
sbinProgs = zfs-zipper
etcFiles = osprey.zfs-zipper.py

prefix=/opt
libDir = ${prefix}/lib
sbinDir = ${prefix}/sbin
etcDir = ${prefix}/etc

installFiles = ${libPyRelFiles:%=${libDir}/%} ${libPycRelFiles:%=${libDir}/%} ${sbinProgs:%=${sbinDir}/%} ${etcFiles:%=${etcDir}/%}

all: ${libPycSrcFiles}

install: ${installFiles}

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
	cp -f $< $@
	chmod a+rx,a-w $@

${etcDir}/%: etc/%
	@mkdir -p $(dir $@)
	cp -f $< $@
	chmod a+r,a-wx $@


%.pyc: %.py
	PYTHONPATH=lib/zfs-zipper ${PYTHON} -B -c 'import compileall;compileall.compile_file("$<")'

clean:
	rm -f ${libPycSrcFiles}
