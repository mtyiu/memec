#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.3.0

if [ $# != 1 ]; then
	echo "Usage: $0 [Number of threads]"
	exit 1
fi

ID=$(hostname | sed 's/testbed-node//g')
RECORD_COUNT=10000000
INSERT_COUNT=$(expr ${RECORD_COUNT} \/ 8)
INSERT_START=$(expr ${INSERT_COUNT} \* \( ${ID} - 11 \) )

${YCSB_PATH}/bin/ycsb \
	load memec \
	-s \
	-P ${YCSB_PATH}/workloads/workloada \
	-p fieldcount=1 \
	-p readallfields=false \
	-p scanproportion=0 \
	-p table=u \
	-p fieldlength=480 \
	-p requestdistribution=zipfian \
	-p recordcount=${RECORD_COUNT} \
	-p insertstart=${INSERT_START} \
	-p insertcount=${INSERT_COUNT} \
	-p threadcount=$1 \
	-p histogram.buckets=200000 \
	-p memec.host=$(hostname -I | xargs) \
	-p memec.port=9112 \
	-p memec.key_size=255 \
	-p memec.chunk_size=4096
