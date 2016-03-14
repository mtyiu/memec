#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.3.0

if [ $# != 2 ]; then
	echo "Usage: $0 [Number of threads] [Workload]"
	exit 1
fi

${YCSB_PATH}/bin/ycsb \
	run memec \
	-s \
	-P ${YCSB_PATH}/workloads/$2 \
	-p fieldcount=1 \
	-p readallfields=false \
	-p scanproportion=0 \
	-p fieldlength=100 \
	-p recordcount=100000 \
	-p operationcount=100000 \
	-p threadcount=$1 \
	-p memec.host=127.0.0.1 \
	-p memec.port=10091 \
	-p memec.key_size=255 \
	-p memec.chunk_size=4096
