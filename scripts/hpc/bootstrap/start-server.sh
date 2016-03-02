#!/bin/bash

SERVER_NAME=$(hostname)
SERVER_IP=$(hostname -I | xargs)
SERVER_PORT=9111
STORAGE_PATH=/tmp/memec/${SERVER_NAME}
CONFIG_PATH=bin/config/hpc
MEMEC_PATH=~/mtyiu/memec

echo "Starting server [${SERVER_NAME}]..."

rm -rf ${STORAGE_PATH}
mkdir -p ${STORAGE_PATH}

cd ${MEMEC_PATH}

if [ $# -gt 0 ]; then
	# Debug mode
	gdb bin/server -ex "r -v \
		-p ${CONFIG_PATH} \
		-o storage path ${STORAGE_PATH} \
		-o server ${SERVER_NAME} tcp://${SERVER_IP}:${SERVER_PORT}/"
else
	bin/server -v \
		-p ${CONFIG_PATH} \
		-o storage path ${STORAGE_PATH} \
		-o server ${SERVER_NAME} tcp://${SERVER_IP}:${SERVER_PORT}/
fi
