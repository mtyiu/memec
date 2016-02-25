#!/bin/bash

if [ $# != 3 ]; then
	echo "Usage: $0 [Data size (bytes)] [Total size (GB)] [Number of clients]"
	exit 1
fi

KEY_SIZE=255
CHUNK_SIZE=40960
BATCH_SIZE=0
DATA_SIZE=$1 # in GB
TOTAL_SIZE=$(expr $2 \* 1073741824) # 250 MB
CLIENT_ID=0
NUM_CLIENTS=1
NUM_THREADS=$3
CLIENT_IP=$(head -n1 scripts/client.conf)
CLIENT_PORTS=$(tail -n +2 scripts/client.conf)

echo "Test case #3 Specification:"
echo "---------------------------"
echo "Data size         : ${DATA_SIZE} bytes"
echo "Total size        : $2 GB (${TOTAL_SIZE} bytes)"
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
	true \
	${CLIENT_IP} \
	${CLIENT_PORTS}
