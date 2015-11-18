#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.3.0

if [ $# != 1 ]; then
	echo "Usage: $0 [Number of threads]"
	exit 1
fi

RECORD_COUNT=100000000

${YCSB_PATH}/bin/ycsb \
	load tachyon \
	-s \
	-P ${YCSB_PATH}/workloads/$2 \
	-p fieldcount=1 \
	-p readallfields=false \
	-p scanproportion=0 \
	-p table=u \
	-p fieldlength=100 \
	-p recordcount=${RECORD_COUNT} \
	-p operationcount=${RECORD_COUNT} \
	-p threadcount=$1 \
	-p uri=tachyon://192.168.0.11:19999
