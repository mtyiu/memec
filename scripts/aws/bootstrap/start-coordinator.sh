#!/bin/bash

CONFIG_PATH=bin/config/aws
MEMEC_PATH=~/mtyiu/memec

echo "Starting coordinator [${COORDINATOR_NAME}]..."

cd ${MEMEC_PATH}

if [ $# -gt 0 ]; then
	# Debug mode
	gdb bin/coordinator -ex "r -v \
		-p ${CONFIG_PATH}"
else
	bin/coordinator \
		-p ${CONFIG_PATH}
fi
