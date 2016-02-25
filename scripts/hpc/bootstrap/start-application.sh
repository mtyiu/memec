#!/bin/bash

MASTER_NAME=$(hostname)
MASTER_IP=$(hostname -I | xargs)
MASTER_PORT=9112
CONFIG_PATH=bin/config/hpc
MEMEC_PATH=~/mtyiu/memec

echo "Starting application [${MASTER_NAME}]..."

cd ${MEMEC_PATH}

if [ $# -gt 0 ]; then
	# Debug mode
	gdb bin/application -ex "r -v \
		-p ${CONFIG_PATH} \
		-o master ${MASTER_NAME} tcp://${MASTER_IP}:${MASTER_PORT}/"
else
	bin/application -v \
		-p ${CONFIG_PATH} \
		-o master ${MASTER_NAME} tcp://${MASTER_IP}:${MASTER_PORT}/
fi

