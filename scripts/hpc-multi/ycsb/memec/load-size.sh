#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.10.0

if [ $# -lt 1 ]; then
	echo "Usage: $0 [Value size]"
	exit 1
fi

ID=$(hostname | sed 's/hpc\([0-9]\+\).cse.cuhk.edu.hk/\1/g')

FIELD_LENGTH=$1
RECORD_COUNT=5000000

if [ "$FIELD_LENGTH" == "1024" ]; then
	RECORD_COUNT=2500000
elif [ "$FIELD_LENGTH" == "2048" ]; then
	RECORD_COUNT=1250000
elif [ "$FIELD_LENGTH" == "4040" ]; then
	RECORD_COUNT=1000000
elif [ "$FIELD_LENGTH" == "4096" ]; then
	RECORD_COUNT=500000
elif [ "$FIELD_LENGTH" == "8192" ]; then
	RECORD_COUNT=250000
elif [ "$FIELD_LENGTH" == "16384" ]; then
	RECORD_COUNT=100000
fi

INSERT_COUNT=$(expr ${RECORD_COUNT} \/ 8)
INSERT_START=$(expr ${INSERT_COUNT} \* $(expr ${ID} - 7 ))

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
	-p insertstart=${INSERT_START} \
	-p insertcount=${INSERT_COUNT} \
	-p threadcount=64 \
	-p memec.host=$(hostname -I | sed 's/^.*\(137\.189\.88\.[0-9]*\).*$/\1/g') \
	-p memec.port=9112 \
	-p memec.key_size=255 \
	-p memec.chunk_size=4096 \
	-p maxexecutiontime=600
