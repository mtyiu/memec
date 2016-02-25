#!/bin/bash

COORDINATOR_NAME=$(hostname | sed 's/testbed-//g')
COORDINATOR_IP=$(hostname -I | xargs)
COORDINATOR_PORT=9110
CONFIG_PATH=bin/config/ncs_huawei
MEMEC_PATH=~/mtyiu/memec

echo "Starting coordinator [${COORDINATOR_NAME}]..."

cd ${MEMEC_PATH}

if [ $# -gt 0 ]; then
	# Debug mode
	gdb bin/coordinator -ex "r -v \
		-p ${CONFIG_PATH} \
		-o coordinator ${COORDINATOR_NAME} tcp://${COORDINATOR_IP}:${COORDINATOR_PORT}/"
else
	bin/coordinator \
		-p ${CONFIG_PATH} \
		-o coordinator ${COORDINATOR_NAME} tcp://${COORDINATOR_IP}:${COORDINATOR_PORT}/
fi
