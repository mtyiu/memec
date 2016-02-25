#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.3.0

if [ $# != 1 ]; then
	echo "Usage: $0 [Number of threads]"
	exit 1
fi

ID=$(echo $STY | sed 's/^.*\(.\)$/\1/g')
MASTER_PORT=$(expr $ID + 9111)

RECORD_COUNT=10000000
INSERT_COUNT=$(expr ${RECORD_COUNT} \/ 4)
if [ $ID == 1 ]; then
	INSERT_START=0
elif [ $ID == 2 ]; then
	INSERT_START=${INSERT_COUNT}
elif [ $ID == 3 ]; then
	INSERT_START=$(expr ${INSERT_COUNT} \* 2)
elif [ $ID == 4 ]; then
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
	-p fieldlength=200 \
	-p requestdistribution=zipfian \
	-p recordcount=${RECORD_COUNT} \
	-p insertstart=${INSERT_START} \
	-p insertcount=${INSERT_COUNT} \
	-p threadcount=$1 \
	-p histogram.buckets=200000 \
	-p memec.host=$(hostname -I | xargs) \
	-p memec.port=${MASTER_PORT} \
	-p memec.key_size=255 \
	-p memec.chunk_size=4096
