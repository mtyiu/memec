#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.3.0

if [ $# != 1 ]; then
	echo "Usage: $0 [Number of threads] [Output file of raw datapoints]"
	exit 1
fi

ID=$(hostname | sed 's/testbed-node//g')
RECORD_COUNT=10000000
INSERT_COUNT=$(expr ${RECORD_COUNT} \/ 8)
INSERT_START=$(expr ${INSERT_COUNT} \* \( ${ID} - 11 \) )

${YCSB_PATH}/bin/ycsb \
	load redis-cluster \
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
