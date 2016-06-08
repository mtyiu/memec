#!/bin/bash

SERVER_NAME=$(hostname | sed 's/testbed-//g')
SERVER_IP=$(hostname -I | sed 's/^.*\(192\.168\.10\.[0-9]*\).*$/\1/g')
SERVER_PORT=9111
STORAGE_PATH=/data/memec_blocks/${SERVER_NAME}
CONFIG_PATH=bin/config/ncs-10g
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
