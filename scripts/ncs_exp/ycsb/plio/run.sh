#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.3.0

if [ $# != 3 ]; then
	echo "Usage: $0 [Number of threads] [Workload] [Output file of raw datapoints]"
	exit 1
fi

RECORD_COUNT=10000000
OPERATION_COUNT=$(expr ${RECORD_COUNT} \/ 4)

${YCSB_PATH}/bin/ycsb \
	run plio \
	-s \
	-P ${YCSB_PATH}/workloads/$2 \
	-p fieldcount=1 \
	-p readallfields=false \
	-p scanproportion=0 \
	-p table=u \
	-p fieldlength=200 \
	-p requestdistribution=zipfian \
	-p recordcount=${RECORD_COUNT} \
	-p operationcount=${OPERATION_COUNT} \
	-p threadcount=$1 \
	-p measurementtype=raw \
	-p measurement.raw.output_file=$3 \
	-p histogram.buckets=200000 \
	-p plio.host=$(hostname -I | xargs) \
	-p plio.port=9112 \
	-p plio.key_size=255 \
	-p plio.chunk_size=4096
