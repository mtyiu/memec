#!/bin/bash

if [ $# != 1 ]; then
	echo "Usage: $0 [Number of clients]"
	exit 1
fi

CLIENT_NAME_PREFIX="$(hostname):"
CLIENT_IP=$(hostname -I | xargs)
CONFIG_PATH=bin/config/hpc
CLIENT_PORT_START=10091
CLIENT_PORT_END=$(expr ${CLIENT_PORT_START} + $1 - 1)
MEMEC_PATH=~/mtyiu/memec

cd ${MEMEC_PATH}

for port in $(seq ${CLIENT_PORT_START} ${CLIENT_PORT_END}); do
	echo "Starting client at port: ${port}..."
	screen -d -m -S client-${port} \
		bin/client -v \
		-p ${CONFIG_PATH} \
		-o client ${CLIENT_NAME_PREFIX}${port} tcp://${CLIENT_IP}:${port}/ &
done

read -p "Press Enter to terminate all clients..."
killall -9 client
