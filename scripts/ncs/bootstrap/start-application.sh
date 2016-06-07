#!/bin/bash

CLIENT_NAME=$(hostname | sed 's/testbed-//g')
CLIENT_IP=$(hostname -I | sed 's/^.*\(192\.168\.0\.[0-9]*\).*$/\1/g')
CLIENT_PORT=9112
CONFIG_PATH=bin/config/ncs
MEMEC_PATH=~/mtyiu/memec

echo "Starting application [${CLIENT_NAME}]..."

cd ${MEMEC_PATH}

if [ $# -gt 0 ]; then
	# Debug mode
	gdb bin/application -ex "r -v \
		-p ${CONFIG_PATH} \
		-o client ${CLIENT_NAME} tcp://${CLIENT_IP}:${CLIENT_PORT}/"
else
	bin/application -v \
		-p ${CONFIG_PATH} \
		-o client ${CLIENT_NAME} tcp://${CLIENT_IP}:${CLIENT_PORT}/
fi
