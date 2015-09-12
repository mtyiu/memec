#!/bin/bash

COORDINATOR_NAME=coordinator # $(hostname)
COORDINATOR_IP=$(hostname -I | xargs)
COORDINATOR_PORT=9110
CONFIG_PATH=bin/config/ncs
PLIO_PATH=~/mtyiu/plio

echo "Starting coordinator [${COORDINATOR_NAME}]..."

cd ${PLIO_PATH}

if [ $# -gt 0 ]; then
	# Debug mode
	gdb bin/coordinator -ex "r -v \
		-p ${CONFIG_PATH} \
		-o coordinator ${COORDINATOR_NAME} tcp://${COORDINATOR_IP}:${COORDINATOR_PORT}/"
else
	bin/coordinator -v \
		-p ${CONFIG_PATH} \
		-o coordinator ${COORDINATOR_NAME} tcp://${COORDINATOR_IP}:${COORDINATOR_PORT}/
fi

