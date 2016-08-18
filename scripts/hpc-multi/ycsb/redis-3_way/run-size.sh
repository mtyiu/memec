#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.10.0

if [ $# != 2 ]; then
	echo "Usage: $0 [Value size] [Workload]"
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
OPERATION_COUNT=$(expr ${INSERT_COUNT} \* 2)

${YCSB_PATH}/bin/ycsb \
	run redis-cs \
	-s \
	-P ${YCSB_PATH}/workloads/$2 \
	-p fieldcount=1 \
	-p readallfields=false \
	-p scanproportion=0 \
	-p table=u \
	-p requestdistribution=zipfian \
	-p fieldlength=${FIELD_LENGTH} \
	-p recordcount=${RECORD_COUNT} \
	-p insertstart=${INSERT_START} \
	-p insertcount=${INSERT_COUNT} \
	-p operationcount=${OPERATION_COUNT} \
	-p threadcount=64 \
	-p redis.serverCount=9 \
	-p redis.host0=137.189.88.38 \
	-p redis.port0=6379 \
	-p redis.host1=137.189.88.39 \
	-p redis.port1=6379 \
	-p redis.host2=137.189.88.40 \
	-p redis.port2=6379 \
	-p redis.host3=137.189.88.41 \
	-p redis.port3=6379 \
	-p redis.host4=137.189.88.42 \
	-p redis.port4=6379 \
	-p redis.host5=137.189.88.43 \
	-p redis.port5=6379 \
	-p redis.host6=137.189.88.44 \
	-p redis.port6=6379 \
	-p redis.host7=137.189.88.45 \
	-p redis.port7=6379 \
	-p redis.host8=137.189.88.46 \
	-p redis.port8=6379 \
	-p maxexecutiontime=600
