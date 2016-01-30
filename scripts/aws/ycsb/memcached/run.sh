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
	run memcached-cluster \
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
	-p memcached.serverCount=10 \
	-p memcached.server0=$(getent hosts node1 | cut -f1 -d' ') \
	-p memcached.port0=11211 \
	-p memcached.server1=$(getent hosts node2 | cut -f1 -d' ') \
	-p memcached.port1=11211 \
	-p memcached.server2=$(getent hosts node3 | cut -f1 -d' ') \
	-p memcached.port2=11211 \
	-p memcached.server3=$(getent hosts node4 | cut -f1 -d' ') \
	-p memcached.port3=11211 \
	-p memcached.server4=$(getent hosts node5 | cut -f1 -d' ') \
	-p memcached.port4=11211 \
	-p memcached.server5=$(getent hosts node6 | cut -f1 -d' ') \
	-p memcached.port5=11211 \
	-p memcached.server6=$(getent hosts node7 | cut -f1 -d' ') \
	-p memcached.port6=11211 \
	-p memcached.server7=$(getent hosts node8 | cut -f1 -d' ') \
	-p memcached.port7=11211 \
	-p memcached.server8=$(getent hosts node9 | cut -f1 -d' ') \
	-p memcached.port8=11211 \
	-p memcached.server9=$(getent hosts node10 | cut -f1 -d' ') \
	-p memcached.port9=11211
