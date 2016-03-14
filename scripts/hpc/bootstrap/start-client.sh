#!/bin/bash

CLIENT_NAME=$(hostname)
CLIENT_IP=$(hostname -I | xargs)
CLIENT_PORT=9112
CONFIG_PATH=bin/config/hpc
MEMEC_PATH=~/mtyiu/memec

echo "Starting client [${CLIENT_NAME}]..."

cd ${MEMEC_PATH}

if [ $# -gt 0 ]; then
	# Debug mode
	gdb bin/client -ex "r -v \
		-p ${CONFIG_PATH} \
		-o client ${CLIENT_NAME} tcp://${CLIENT_IP}:${CLIENT_PORT}/"
else
	bin/client -v \
		-p ${CONFIG_PATH} \
		-o client ${CLIENT_NAME} tcp://${CLIENT_IP}:${CLIENT_PORT}/
fi
