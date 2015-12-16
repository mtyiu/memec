#!/bin/bash

if [ $# != 4 ]; then
	echo "Usage: $0 [Data size] [Client ID] [Number of clients] [Number of threads]"
	exit 1
fi

KEY_SIZE=255
CHUNK_SIZE=4096
BATCH_SIZE=0
DATA_SIZE=$1
TOTAL_SIZE=26214400 # 250 MB
CLIENT_ID=$2
NUM_CLIENTS=$3
NUM_THREADS=$4
MASTER_IP=$(head -n1 scripts/master.conf)
MASTER_PORT=$(tail -n +2 scripts/master.conf)

if [ ${CLIENT_ID} -ge ${NUM_CLIENTS} ]; then
	echo "The specified client ID is invalid. It should ranges from [0..$(expr ${NUM_CLIENTS} - 1)]"
	exit 1
fi

echo "Test case #2 Specification:"
echo "---------------------------"
echo "Data size         : ${DATA_SIZE} bytes"
echo "Total size        : ${TOTAL_SIZE} bytes"
echo "Client ID         : ${CLIENT_ID}"
echo "Number of clients : ${NUM_CLIENTS}"
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
	${MASTER_IP} \
	${MASTER_PORTS}
