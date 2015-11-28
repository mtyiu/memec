#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.3.0

if [ $# != 3 ]; then
	echo "Usage: $0 [Number of threads] [Total data size] [Field length]"
	exit 1
fi

TOTAL_DATA_SIZE=$2
FIELD_LENGTH=$3
ID=$(hostname | sed 's/testbed-node//g')
RECORD_COUNT=$(expr ${TOTAL_DATA_SIZE} \/ ${FIELD_LENGTH})
INSERT_COUNT=$(expr ${RECORD_COUNT} \/ 4)
if [ $ID == 3 ]; then
	INSERT_START=0
elif [ $ID == 4 ]; then
	INSERT_START=${INSERT_COUNT}
elif [ $ID == 8 ]; then
	INSERT_START=$(expr ${INSERT_COUNT} \* 2)
elif [ $ID == 9 ]; then
	INSERT_START=$(expr ${INSERT_COUNT} \* 3)
fi

${YCSB_PATH}/bin/ycsb \
	load plio \
	-s \
	-P ${YCSB_PATH}/workloads/workloada \
	-p fieldcount=1 \
	-p readallfields=false \
	-p scanproportion=0 \
	-p table=u \
	-p fieldlength=${FIELD_LENGTH} \
	-p requestdistribution=zipfian \
	-p recordcount=${RECORD_COUNT} \
	-p insertstart=${INSERT_START} \
	-p insertcount=${INSERT_COUNT} \
	-p threadcount=$1 \
	-p histogram.buckets=10 \
	-p plio.host=$(hostname -I | xargs) \
	-p plio.port=9112 \
	-p plio.key_size=255 \
	-p plio.chunk_size=4096