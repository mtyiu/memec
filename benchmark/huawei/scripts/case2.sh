#!/bin/bash

if [ $# != 2 ]; then
	echo "Usage: $0 [Data size] [Number of clients]"
	# echo "Usage: $0 [Data size] [Client ID] [Number of clients] [Number of threads]"
	exit 1
fi

KEY_SIZE=255
CHUNK_SIZE=40960
BATCH_SIZE=0
DATA_SIZE=$1
TOTAL_SIZE=1073741824 # 1 GB
CLIENT_ID=0 #$2
NUM_CLIENTS=1 #$3
NUM_THREADS=$2
CLIENT_IP=$(head -n1 scripts/client.conf)
CLIENT_PORTS=$(tail -n +2 scripts/client.conf)

if [ ${CLIENT_ID} -ge ${NUM_CLIENTS} ]; then
	echo "The specified client ID is invalid. It should ranges from [0..$(expr ${NUM_CLIENTS} - 1)]"
	exit 1
fi

echo "Test case #2 Specification:"
echo "---------------------------"
echo "Data size         : ${DATA_SIZE} bytes"
echo "Total size        : ${TOTAL_SIZE} bytes"
# echo "Client ID         : ${CLIENT_ID}"
# echo "Number of clients : ${NUM_CLIENTS}"
echo "Number of threads : ${NUM_THREADS}"
echo "Batch size        : Disabled"
echo

bin/benchmark \
	${KEY_SIZE} \
	${CHUNK_SIZE} \
	${BATCH_SIZE} \
	${DATA_SIZE} \
	${TOTAL_SIZE} \
	${CLIENT_ID} \
	${NUM_CLIENTS} \
	${NUM_THREADS} \
	false \
	${CLIENT_IP} \
	${CLIENT_PORTS}
