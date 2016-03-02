#!/bin/bash

SLAVE_NAME=$(hostname | sed 's/testbed-//g')
SLAVE_IP=$(hostname -I | awk '{print $1}' | xargs)
SLAVE_PORT=9111
STORAGE_PATH=/tmp/memec/${SLAVE_NAME}
CONFIG_PATH=bin/config/ncs
MEMEC_PATH=~/mtyiu/memec

echo "Starting backup slave [${SLAVE_NAME}]..."

rm -rf ${STORAGE_PATH}
mkdir -p ${STORAGE_PATH}

cd ${MEMEC_PATH}

if [ $# -gt 0 ]; then
	# Debug mode
	gdb bin/server -ex "r -v \
		-p ${CONFIG_PATH} \
		-o pool chunks 4294967296 \
		-o storage path ${STORAGE_PATH} \
		-o slave ${SLAVE_NAME} tcp://${SLAVE_IP}:${SLAVE_PORT}/"
else
	bin/server -v \
		-p ${CONFIG_PATH} \
		-o pool chunks 4294967296 \
		-o storage path ${STORAGE_PATH} \
		-o slave ${SLAVE_NAME} tcp://${SLAVE_IP}:${SLAVE_PORT}/
fi
