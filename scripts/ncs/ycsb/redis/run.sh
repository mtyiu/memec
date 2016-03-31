#!/bin/bash

###################################################
#
# Run the workload using YCSB client
# INPUT: (1) Number of threads to use in each client, (2) name of the YCSB workload
#
###################################################

YCSB_PATH=~/mtyiu/ycsb/0.7.0

if [ $# != 2 ]; then
	echo "Usage: $0 [Number of threads] [Workload] [Output file of raw datapoints]"
	exit 1
fi

# Evenly distribute the # of ops to YCSB clients ( 4 in the experiment setting )
RECORD_COUNT=10000000
OPERATION_COUNT=$(expr ${RECORD_COUNT} \/ 4)

# Run the target workload
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
	-p operationcount=${OPERATION_COUNT} \
	-p threadcount=$1 \
	-p histogram.buckets=200000 \
	-p redis.serverCount=16 \
	-p redis.host0=192.168.0.22 \
	-p redis.port0=6379 \
	-p redis.host1=192.168.0.21 \
	-p redis.port1=6379 \
	-p redis.host2=192.168.0.23 \
	-p redis.port2=6379 \
	-p redis.host3=192.168.0.24 \
	-p redis.port3=6379 \
	-p redis.host4=192.168.0.25 \
	-p redis.port4=6379 \
	-p redis.host5=192.168.0.26 \
	-p redis.port5=6379 \
	-p redis.host6=192.168.0.27 \
	-p redis.port6=6379 \
	-p redis.host7=192.168.0.28 \
	-p redis.port7=6379 \
	-p redis.host8=192.168.0.29 \
	-p redis.port8=6379 \
	-p redis.host9=192.168.0.30 \
	-p redis.port9=6379 \
	-p redis.host10=192.168.0.31 \
	-p redis.port10=6379 \
	-p redis.host11=192.168.0.32 \
	-p redis.port11=6379 \
	-p redis.host12=192.168.0.33 \
	-p redis.port12=6379 \
	-p redis.host13=192.168.0.47 \
	-p redis.port13=6379 \
	-p redis.host14=192.168.0.48 \
	-p redis.port14=6379 \
	-p redis.host15=192.168.0.49 \
	-p redis.port15=6379
