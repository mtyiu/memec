#!/bin/bash

CONFIG_PATH=bin/config/aws
PLIO_PATH=~/mtyiu/plio

echo "Starting coordinator [${COORDINATOR_NAME}]..."

cd ${PLIO_PATH}

if [ $# -gt 0 ]; then
	# Debug mode
	gdb bin/coordinator -ex "r -v \
		-p ${CONFIG_PATH}"
else
	bin/coordinator \
		-p ${CONFIG_PATH}
fi
