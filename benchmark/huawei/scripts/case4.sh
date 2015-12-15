#!/bin/bash

if [ $# != 5 ]; then
	echo "Usage: $0 [Data size (bytes)] [Total size (GB)] [Number of threads] [Client ID] [Number of clients]"
	exit 1
fi

KEY_SIZE=255
CHUNK_SIZE=4096
BATCH_SIZE=0
DATA_SIZE=$1 # in GB
TOTAL_SIZE=$(expr $2 \* 1073741824) # 250 MB
MASTER_IP=$(head -n1 scripts/master.conf)
MASTER_PORT=$(tail -n1 scripts/master.conf)
CLIENT_ID=$4
NUM_CLIENTS=$5
NUM_THREADS=$3

if [ ${CLIENT_ID} -ge ${NUM_CLIENTS} ]; then
	echo "The specified client ID is invalid. It should ranges from [0..$(expr ${NUM_CLIENTS} - 1)]"
	exit 1
fi

echo "Test case #4 Specification:"
echo "---------------------------"
echo "Data size         : ${DATA_SIZE} bytes"
echo "Total size        : $2 GB (${TOTAL_SIZE} bytes)"
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
	${MASTER_IP} \
	${MASTER_PORT} \
	${CLIENT_ID} \
	${NUM_CLIENTS} \
	${NUM_THREADS} \
	true
