NAME=           dockerbox-broker
PREFIX?=        /usr/local
SBINDIR?=       ${PREFIX}/sbin
RCDDIR?=        ${PREFIX}/etc/rc.d
ETCDIR?=        ${PREFIX}/etc/${NAME}
BUILDDIR?=      build

GO?=            go
GOFLAGS?=

.PHONY: all build install clean

all: build

build:
	${GO} build ${GOFLAGS} -o ${BUILDDIR}/${NAME} .

install: build
	mkdir -p ${BUILDDIR}
	install -m 755 ${BUILDDIR}/${NAME} ${SBINDIR}/${NAME}
	install -m 755 rc.d/${NAME} ${RCDDIR}/${NAME}
	mkdir -p ${ETCDIR}
	install -m 644 .env.sample ${ETCDIR}/${NAME}.env

clean:
	rm -f ${NAME}
