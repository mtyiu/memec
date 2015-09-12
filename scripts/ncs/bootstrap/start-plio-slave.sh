#!/bin/bash

SLAVE_NAME=$(hostname)
SLAVE_IP=$(hostname -I | xargs)
SLAVE_PORT=9111
STORAGE_PATH=/tmp/plio/${SLAVE_NAME}
CONFIG_PATH=bin/config/ncs
PLIO_PATH=~/mtyiu/plio

echo "Starting slave [${SLAVE_NAME}]..."

rm -rf ${STORAGE_PATH}
mkdir -p ${STORAGE_PATH}

cd ${PLIO_PATH}

if [ $# -gt 0 ]; then
	# Debug mode
	gdb bin/slave -ex "r -v \
		-p ${CONFIG_PATH} \
		-o storage path ${STORAGE_PATH} \
		-o slave ${SLAVE_NAME} tcp://${SLAVE_IP}:${SLAVE_PORT}/"
else
	bin/slave -v \
		-p ${CONFIG_PATH} \
		-o storage path ${STORAGE_PATH} \
		-o slave ${SLAVE_NAME} tcp://${SLAVE_IP}:${SLAVE_PORT}/
fi
