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
RECORD_COUNT=5000000
INSERT_COUNT=$(expr ${RECORD_COUNT} \/ 2)
if [ $ID == 31 ]; then
	INSERT_START=0
	EXTRA_OP="-p fieldlength=8 -p table=a"
elif [ $ID == 32 ]; then
	INSERT_START=${INSERT_COUNT}
	EXTRA_OP="-p fieldlength=8 -p table=a"
elif [ $ID == 33 ]; then
	INSERT_START=0
	EXTRA_OP="-p fieldlength=32 -p table=b"
elif [ $ID == 34 ]; then
	INSERT_START=${INSERT_COUNT}
	EXTRA_OP="-p fieldlength=32 -p table=b"
fi

# Load the store with key-value pairs
${YCSB_PATH}/bin/ycsb \
	load redis-cs \
	-s \
	-P ${YCSB_PATH}/workloads/workloada \
	-p fieldcount=1 \
	-p readallfields=false \
	-p scanproportion=0 \
	-p requestdistribution=zipfian \
	-p recordcount=${RECORD_COUNT} \
	-p insertstart=${INSERT_START} \
	-p insertcount=${INSERT_COUNT} \
	-p threadcount=$1 \
	-p histogram.buckets=200000 \
	-p zeropadding=19 \
	-p redis.serverCount=10 \
	-p redis.host0=192.168.10.22 \
	-p redis.port0=6379 \
	-p redis.host1=192.168.10.21 \
	-p redis.port1=6379 \
	-p redis.host2=192.168.10.23 \
	-p redis.port2=6379 \
	-p redis.host3=192.168.10.24 \
	-p redis.port3=6379 \
	-p redis.host4=192.168.10.25 \
	-p redis.port4=6379 \
	-p redis.host5=192.168.10.26 \
	-p redis.port5=6379 \
	-p redis.host6=192.168.10.27 \
	-p redis.port6=6379 \
	-p redis.host7=192.168.10.28 \
	-p redis.port7=6379 \
	-p redis.host8=192.168.10.29 \
	-p redis.port8=6379 \
	-p redis.host9=192.168.10.30 \
	-p redis.port9=6379 \
	${EXTRA_OP}
