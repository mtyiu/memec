#!/bin/bash

SLAVE_NAME=$(hostname | sed 's/testbed-//g')
SLAVE_IP=$(hostname -I | awk '{print $1}' | xargs)
SLAVE_PORT=9111
STORAGE_PATH=/data/memec_blocks/${SLAVE_NAME}
CONFIG_PATH=bin/config/ncs_exp
PLIO_PATH=~/mtyiu/plio

echo "Starting slave [${SLAVE_NAME}]..."

rm -rf ${STORAGE_PATH}
mkdir -p ${STORAGE_PATH}

cd ${PLIO_PATH}

if [ $# -gt 0 ]; then
	# Debug mode
	gdb bin/server -ex "r -v \
		-p ${CONFIG_PATH} \
		-o storage path ${STORAGE_PATH} \
		-o slave ${SLAVE_NAME} tcp://${SLAVE_IP}:${SLAVE_PORT}/"
else
	bin/server -v \
		-p ${CONFIG_PATH} \
		-o storage path ${STORAGE_PATH} \
		-o slave ${SLAVE_NAME} tcp://${SLAVE_IP}:${SLAVE_PORT}/
fi
