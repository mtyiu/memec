#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.7.0

if [ $# != 2 ]; then
	echo "Usage: $0 [Number of threads] [Workload]"
	exit 1
fi

ID=$(hostname | sed 's/testbed-node//g')
RECORD_COUNT=5000000
INSERT_COUNT=$(expr ${RECORD_COUNT} \/ 2)
OPERATION_COUNT=$(expr ${RECORD_COUNT} \* 2 \/ 2)
if [ $ID == 3 ]; then
	INSERT_START=0
	EXTRA_OP="-p table=a -p fieldlength=8"
elif [ $ID == 4 ]; then
	INSERT_START=${INSERT_COUNT}
	EXTRA_OP="-p table=a -p fieldlength=8"
elif [ $ID == 8 ]; then
	INSERT_START=0
	EXTRA_OP="-p table=b -p fieldlength=32"
elif [ $ID == 9 ]; then
	INSERT_START=${INSERT_COUNT}
	EXTRA_OP="-p table=b -p fieldlength=32"
fi

${YCSB_PATH}/bin/ycsb \
	run memec \
	-s \
	-P ${YCSB_PATH}/workloads/workloada \
	-p fieldcount=1 \
	-p readallfields=false \
	-p scanproportion=0 \
	-p readproportion=0.34 \
	-p updateproportion=0.33 \
	-p insertproportion=0.33 \
	-p requestdistribution=zipfian \
	-p recordcount=${RECORD_COUNT} \
	-p insertstart=${INSERT_START} \
	-p insertcount=${INSERT_COUNT} \
	-p operationcount=${OPERATION_COUNT} \
	-p threadcount=$1 \
	-p histogram.buckets=200000 \
	-p memec.host=$(hostname -I | xargs) \
	-p memec.port=9112 \
	-p memec.key_size=255 \
	-p memec.chunk_size=4096 \
	-p measurementtype=timeseries \
	-p timeseries.granularity=500 \
	-p zeropadding=19 \
	${EXTRA_OP}
