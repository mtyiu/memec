#!/bin/bash

MASTER_ID=$(echo $STY | sed 's/^.*\(.\)$/\1/g')
MASTER_NAME=$(echo $STY | sed 's/[0-9]\+\.//g')
MASTER_IP=$(hostname -I | awk '{print $1}' | xargs)
MASTER_PORT=$(expr $MASTER_ID + 9111)
CONFIG_PATH=bin/config/aws
PLIO_PATH=~/mtyiu/plio

echo "Starting master [${MASTER_NAME}]..."

cd ${PLIO_PATH}

if [ $# -gt 0 ]; then
	# Debug mode
	gdb bin/client -ex "r -v \
		-p ${CONFIG_PATH} \
		-o master ${MASTER_NAME} tcp://${MASTER_IP}:${MASTER_PORT}/"
else
	bin/client -v \
		-p ${CONFIG_PATH} \
		-o master ${MASTER_NAME} tcp://${MASTER_IP}:${MASTER_PORT}/
fi
