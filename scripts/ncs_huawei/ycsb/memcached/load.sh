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
	load memcached-cluster \
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
	-p memcached.serverCount=8 \
	-p memcached.server0=192.168.0.29 \
	-p memcached.port0=11211 \
	-p memcached.server1=192.168.0.30 \
	-p memcached.port1=11211 \
	-p memcached.server2=192.168.0.31 \
	-p memcached.port2=11211 \
	-p memcached.server3=192.168.0.32 \
	-p memcached.port3=11211 \
	-p memcached.server4=192.168.0.33 \
	-p memcached.port4=11211 \
	-p memcached.server5=192.168.0.47 \
	-p memcached.port5=11211 \
	-p memcached.server6=192.168.0.48 \
	-p memcached.port6=11211 \
	-p memcached.server7=192.168.0.49 \
	-p memcached.port7=11211
