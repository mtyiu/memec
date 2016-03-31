#!/bin/bash

###################################################
#
# Run the workload using YCSB client
# INPUT: (1) Number of threads to use in each client, (2) name of the YCSB workload
#
###################################################

YCSB_PATH=~/mtyiu/ycsb/0.7.0

if [ $# != 2 ]; then
	echo "Usage: $0 [Number of threads] [Workload]"
	exit 1
fi

# Evenly distribute the # of ops to YCSB clients ( 4 in the experiment setting )
RECORD_COUNT=10000000
OPERATION_COUNT=$(expr ${RECORD_COUNT} \/ 4)

# Run the target workload
${YCSB_PATH}/bin/ycsb \
	run memcached \
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
	-p memcached.hosts=192.168.0.21,192.168.0.22,192.168.0.23,192.168.0.24,192.168.0.25,192.168.0.26,192.168.0.27,192.168.0.28,192.168.0.29,192.168.0.30,192.168.0.31,192.168.0.32,192.168.0.33,192.168.0.47,192.168.0.48,192.168.0.49
