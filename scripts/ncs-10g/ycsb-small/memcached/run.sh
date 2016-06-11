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

ID=$(hostname | sed 's/testbed-node//g')

# Evenly distribute the # of ops to YCSB clients ( 4 in the experiment setting )
RECORD_COUNT=5000000
OPERATION_COUNT=$(expr ${RECORD_COUNT} \* 2 \/ 2)
if [ $ID == 31 ]; then
	EXTRA_OP="-p fieldlength=8 -p table=a"
elif [ $ID == 32 ]; then
	EXTRA_OP="-p fieldlength=8 -p table=a"
elif [ $ID == 33 ]; then
	EXTRA_OP="-p fieldlength=32 -p table=b"
elif [ $ID == 34 ]; then
	EXTRA_OP="-p fieldlength=32 -p table=b"
fi

# Run the target workload
${YCSB_PATH}/bin/ycsb \
	run memcached \
	-s \
	-jvm-args '\-Xmx3584m' \
	-P ${YCSB_PATH}/workloads/$2 \
	-p fieldcount=1 \
	-p readallfields=false \
	-p scanproportion=0 \
	-p requestdistribution=zipfian \
	-p recordcount=${RECORD_COUNT} \
	-p operationcount=${OPERATION_COUNT} \
	-p threadcount=$1 \
	-p histogram.buckets=200000 \
	-p zeropadding=19 \
	-p memcached.readBufferSize=524288 \
	-p memcached.hosts=192.168.10.21,192.168.10.22,192.168.10.23,192.168.10.24,192.168.10.25,192.168.10.26,192.168.10.27,192.168.10.28,192.168.10.29,192.168.10.30 \
	${EXTRA_OP}
