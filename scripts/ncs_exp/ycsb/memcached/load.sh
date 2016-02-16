#!/bin/bash

###################################################
#
# Load key-value pairs using YCSB client
# INPUT: (1) Number of threads to use in each client, (2) name of the YCSB workload
#
###################################################

YCSB_PATH=~/mtyiu/ycsb/0.3.0

if [ $# != 1 ]; then
	echo "Usage: $0 [Number of threads] [Output file of raw datapoints]"
	exit 1
fi

ID=$(hostname | sed 's/testbed-node//g')

# Evenly distribute the # of ops to YCSB clients ( 4 in the experiment setting )
# Each client inserts pairs of an independent key range
RECORD_COUNT=10000000
INSERT_COUNT=$(expr ${RECORD_COUNT} \/ 4)
if [ $ID == 3 ]; then
	INSERT_START=0
elif [ $ID == 4 ]; then
	INSERT_START=${INSERT_COUNT}
elif [ $ID == 8 ]; then
	INSERT_START=$(expr ${INSERT_COUNT} \* 2)
elif [ $ID == 9 ]; then
	INSERT_START=$(expr ${INSERT_COUNT} \* 3)
fi

# Load the store with key-value pairs
${YCSB_PATH}/bin/ycsb \
	load memcached-cluster \
	-s \
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
	-p memcached.serverCount=16 \
	-p memcached.server0=192.168.0.22 \
	-p memcached.port0=11211 \
	-p memcached.server1=192.168.0.21 \
	-p memcached.port1=11211 \
	-p memcached.server2=192.168.0.23 \
	-p memcached.port2=11211 \
	-p memcached.server3=192.168.0.24 \
	-p memcached.port3=11211 \
	-p memcached.server4=192.168.0.25 \
	-p memcached.port4=11211 \
	-p memcached.server5=192.168.0.26 \
	-p memcached.port5=11211 \
	-p memcached.server6=192.168.0.27 \
	-p memcached.port6=11211 \
	-p memcached.server7=192.168.0.28 \
	-p memcached.port7=11211 \
	-p memcached.server8=192.168.0.29 \
	-p memcached.port8=11211 \
	-p memcached.server9=192.168.0.30 \
	-p memcached.port9=11211 \
	-p memcached.server10=192.168.0.31 \
	-p memcached.port10=11211 \
	-p memcached.server11=192.168.0.32 \
	-p memcached.port11=11211 \
	-p memcached.server12=192.168.0.33 \
	-p memcached.port12=11211 \
	-p memcached.server13=192.168.0.47 \
	-p memcached.port13=11211 \
	-p memcached.server14=192.168.0.48 \
	-p memcached.port14=11211 \
	-p memcached.server15=192.168.0.49 \
	-p memcached.port15=11211
