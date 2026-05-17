NAME=           docker-broker
PREFIX?=        /usr/local
SBINDIR?=       ${PREFIX}/sbin
RCDDIR?=        ${PREFIX}/etc/rc.d
ETCDIR?=        ${PREFIX}/etc/${NAME}

GO?=            go
GOFLAGS?=

.PHONY: all build install clean

all: build

build:
	${GO} build ${GOFLAGS} -o ${NAME} .

install: build
	install -m 755 ${NAME} ${SBINDIR}/${NAME}
	install -m 755 rc.d/${NAME} ${RCDDIR}/${NAME}
	mkdir -p ${ETCDIR}
	install -m 644 .env.sample ${ETCDIR}/${NAME}.env

clean:
	rm -f ${NAME}
