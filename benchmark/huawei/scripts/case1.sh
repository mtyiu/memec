#!/bin/bash

if [ $# != 5 ]; then
	echo "Usage: $0 [Batch size] [Number of windows] [Window rate (items per second)] [Items per window] [Number of threads]"
	exit 1
fi

KEY_SIZE=255
CHUNK_SIZE=40960
BATCH_SIZE=$1
DATA_SIZE=512
NUM_WINDOWS=$2
WINDOW_RATE=$3
ITEMS_PER_WINDOW=$4
CLIENT_ID=0
NUM_CLIENTS=1
NUM_THREADS=$5
CLIENT_IP=$(head -n1 scripts/client.conf)
CLIENT_PORTS=$(tail -n +2 scripts/client.conf)

if [ ${CLIENT_ID} -ge ${NUM_CLIENTS} ]; then
	echo "The specified client ID is invalid. It should ranges from [0..$(expr ${NUM_CLIENTS} - 1)]"
	exit 1
fi

echo "Test case #1 Specification:"
echo "---------------------------"
echo "Data size         : ${DATA_SIZE} bytes"
echo "Batch size        : ${BATCH_SIZE}"
echo "Number of windows : ${NUM_WINDOWS}"
echo "Window rate       : ${WINDOW_RATE} items per second"
echo "Items per window  : ${ITEMS_PER_WINDOW}"
echo "Number of threads : ${NUM_THREADS}"
echo

bin/window \
	${KEY_SIZE} \
	${CHUNK_SIZE} \
	${BATCH_SIZE} \
	${DATA_SIZE} \
	${NUM_WINDOWS} \
	${WINDOW_RATE} \
	${ITEMS_PER_WINDOW} \
	${CLIENT_ID} \
	${NUM_CLIENTS} \
	${NUM_THREADS} \
	${CLIENT_IP} \
	${CLIENT_PORTS}
