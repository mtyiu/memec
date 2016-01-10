#!/bin/bash

MASTER_NAME=$(hostname | sed 's/testbed-//g')
MASTER_IP=$(hostname -I | awk '{print $1}' | xargs)
MASTER_PORT=9112
CONFIG_PATH=bin/config/ncs_exp
PLIO_PATH=~/mtyiu/plio

echo "Starting master [${MASTER_NAME}]..."

cd ${PLIO_PATH}

if [ $# -gt 0 ]; then
	# Debug mode
	gdb bin/master -ex "r -v \
		-p ${CONFIG_PATH} \
		-o master ${MASTER_NAME} tcp://${MASTER_IP}:${MASTER_PORT}/"
else
	bin/master -v \
		-p ${CONFIG_PATH} \
		-o master ${MASTER_NAME} tcp://${MASTER_IP}:${MASTER_PORT}/
fi
