#!/bin/bash

###################################################
#
# Load key-value pairs using YCSB client
# INPUT: (1) Number of threads to use in each client, (2) name of the YCSB workload
#
###################################################

YCSB_PATH=~/mtyiu/ycsb/0.7.0

if [ $# != 1 ]; then
	echo "Usage: $0 [Number of threads] [Output file of raw datapoints]"
	exit 1
fi

ID=$(hostname | sed 's/testbed-node//g')

# Evenly distribute the # of ops to YCSB clients ( 4 in the experiment setting )
# Each client inserts pairs of an independent key range
RECORD_COUNT=10000000
INSERT_COUNT=$(expr ${RECORD_COUNT} \/ 4)
if [ $ID == 31 ]; then
	INSERT_START=0
elif [ $ID == 32 ]; then
	INSERT_START=${INSERT_COUNT}
elif [ $ID == 33 ]; then
	INSERT_START=$(expr ${INSERT_COUNT} \* 2)
elif [ $ID == 34 ]; then
	INSERT_START=$(expr ${INSERT_COUNT} \* 3)
fi

# Load the store with key-value pairs
${YCSB_PATH}/bin/ycsb \
	load memcached \
	-s \
	-jvm-args '\-Xmx3584m' \
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
	-p memcached.readBufferSize=524288 \
	-p memcached.hosts=192.168.10.21,192.168.10.22,192.168.10.23,192.168.10.24,192.168.10.25,192.168.10.26,192.168.10.27,192.168.10.28,192.168.10.29,192.168.10.30
