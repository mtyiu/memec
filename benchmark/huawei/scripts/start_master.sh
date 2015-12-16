#!/bin/bash

if [ $# != 1 ]; then
	echo "Usage: $0 [Number of masters]"
	exit 1
fi

MASTER_NAME_PREFIX="$(hostname):"
MASTER_IP=$(hostname -I | xargs)
CONFIG_PATH=bin/config/hpc
MASTER_PORT_START=10091
MASTER_PORT_END=$(expr ${MASTER_PORT_START} + $1 - 1)
PLIO_PATH=~/mtyiu/plio

cd ${PLIO_PATH}

for port in $(seq ${MASTER_PORT_START} ${MASTER_PORT_END}); do
	echo "Starting master at port: ${port}..."
	screen -d -m -S master-${port} \
		bin/master -v \
		-p ${CONFIG_PATH} \
		-o master ${MASTER_NAME_PREFIX}${port} tcp://${MASTER_IP}:${port}/ &
done

read -p "Press Enter to terminate all masters..."
killall -9 master
