#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.3.0

if [ $# != 2 ]; then
	echo "Usage: $0 [Number of threads] [Workload] [Output file of raw datapoints]"
	exit 1
fi

RECORD_COUNT=10000000
OPERATION_COUNT=$(expr ${RECORD_COUNT} \/ 4)

${YCSB_PATH}/bin/ycsb \
	run redis-cluster \
	-s \
	-P ${YCSB_PATH}/workloads/$2 \
	-p fieldcount=1 \
	-p readallfields=false \
	-p scanproportion=0 \
	-p table=u \
	-p fieldlength=480 \
	-p requestdistribution=zipfian \
	-p recordcount=${RECORD_COUNT} \
	-p operationcount=${OPERATION_COUNT} \
	-p threadcount=$1 \
	-p histogram.buckets=200000 \
	-p redis.serverCount=8 \
	-p redis.host0=192.168.0.29 \
	-p redis.port0=6379 \
	-p redis.host1=192.168.0.30 \
	-p redis.port1=6379 \
	-p redis.host2=192.168.0.31 \
	-p redis.port2=6379 \
	-p redis.host3=192.168.0.32 \
	-p redis.port3=6379 \
	-p redis.host4=192.168.0.33 \
	-p redis.port4=6379 \
	-p redis.host5=192.168.0.47 \
	-p redis.port5=6379 \
	-p redis.host6=192.168.0.48 \
	-p redis.port6=6379 \
	-p redis.host7=192.168.0.49 \
	-p redis.port7=6379
