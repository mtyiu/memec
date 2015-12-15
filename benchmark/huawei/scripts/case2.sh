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
MASTER_IP=127.0.0.1
MASTER_PORT=10091
CLIENT_ID=$2
NUM_CLIENTS=$3
NUM_THREADS=$4

bin/case2 \
	${KEY_SIZE} \
	${CHUNK_SIZE} \
	${BATCH_SIZE} \
	${DATA_SIZE} \
	${TOTAL_SIZE} \
	${MASTER_IP} \
	${MASTER_PORT} \
	${CLIENT_ID} \
	${NUM_CLIENTS} \
	${NUM_THREADS}
