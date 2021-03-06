#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.7.0

if [ $# != 3 ]; then
	echo "Usage: $0 [Number of threads] [Total data size] [Field length]"
	exit 1
fi

TOTAL_DATA_SIZE=$2
FIELD_LENGTH=$3
ID=$(hostname | sed 's/testbed-node//g')
RECORD_COUNT=$(expr ${TOTAL_DATA_SIZE} \/ ${FIELD_LENGTH})
INSERT_COUNT=$(expr ${RECORD_COUNT} \/ 4)
if [ $ID == 31 ]; then
	INSERT_START=0
elif [ $ID == 32 ]; then
	INSERT_START=${INSERT_COUNT}
elif [ $ID == 33 ]; then
	INSERT_START=$(expr ${INSERT_COUNT} \* 2)
elif [ $ID == 34 ]; then
	INSERT_START=$(expr ${INSERT_COUNT} \* 3)
fi

${YCSB_PATH}/bin/ycsb \
	load memec \
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
	-p memec.host=$(hostname -I | sed 's/^.*\(192\.168\.10\.[0-9]*\).*$/\1/g') \
	-p memec.port=9112 \
	-p memec.key_size=255 \
	-p memec.chunk_size=4096
