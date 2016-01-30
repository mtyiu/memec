#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.3.0

if [ $# != 2 ]; then
	echo "Usage: $0 [Number of threads] [Workload] [Output file of raw datapoints]"
	exit 1
fi

ID=$(echo $STY | sed 's/^.*\(.\)$/\1/g')
RECORD_COUNT=10000000
INSERT_COUNT=$(expr ${RECORD_COUNT} \/ 4)
OPERATION_COUNT=$(expr ${RECORD_COUNT} \/ 4)
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
	run redis-cluster \
	-s \
	-P ${YCSB_PATH}/workloads/$2 \
	-p fieldcount=1 \
	-p readallfields=false \
	-p scanproportion=0 \
	-p table=u \
	-p fieldlength=200 \
	-p requestdistribution=zipfian \
	-p recordcount=${RECORD_COUNT} \
	-p insertstart=${INSERT_START} \
	-p insertcount=${INSERT_COUNT} \
	-p operationcount=${OPERATION_COUNT} \
	-p threadcount=$1 \
	-p histogram.buckets=200000 \
	-p redis.serverCount=10 \
	-p redis.host0=$(getent hosts node1 | cut -f1 -d' ') \
	-p redis.port0=6379 \
	-p redis.host1=$(getent hosts node2 | cut -f1 -d' ') \
	-p redis.port1=6379 \
	-p redis.host2=$(getent hosts node3 | cut -f1 -d' ') \
	-p redis.port2=6379 \
	-p redis.host3=$(getent hosts node4 | cut -f1 -d' ') \
	-p redis.port3=6379 \
	-p redis.host4=$(getent hosts node5 | cut -f1 -d' ') \
	-p redis.port4=6379 \
	-p redis.host5=$(getent hosts node6 | cut -f1 -d' ') \
	-p redis.port5=6379 \
	-p redis.host6=$(getent hosts node7 | cut -f1 -d' ') \
	-p redis.port6=6379 \
	-p redis.host7=$(getent hosts node8 | cut -f1 -d' ') \
	-p redis.port7=6379 \
	-p redis.host8=$(getent hosts node9 | cut -f1 -d' ') \
	-p redis.port8=6379 \
	-p redis.host9=$(getent hosts node10 | cut -f1 -d' ') \
	-p redis.port9=6379
