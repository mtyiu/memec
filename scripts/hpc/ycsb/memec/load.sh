#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.10.0

if [ $# -lt 1 ]; then
	echo "Usage: $0 [Number of threads]"
	exit 1
fi

FIELD_LENGTH=100
RECORD_COUNT=5000000

${YCSB_PATH}/bin/ycsb \
	load memec \
	-s \
	-P ${YCSB_PATH}/workloads/workloada \
	-p fieldcount=1 \
	-p readallfields=false \
	-p scanproportion=0 \
	-p table=u \
	-p requestdistribution=zipfian \
	-p fieldlength=${FIELD_LENGTH} \
	-p recordcount=${RECORD_COUNT} \
	-p threadcount=$1 \
	-p memec.host=137.189.88.46 \
	-p memec.port=9112 \
	-p memec.key_size=255 \
	-p memec.chunk_size=4096 \
	-p maxexecutiontime=600
