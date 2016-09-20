#!/bin/bash

###################################################
#
# Run the workload using YCSB client
# INPUT: (1) Number of threads to use in each client, (2) name of the YCSB workload
#
###################################################

YCSB_PATH=~/mtyiu/ycsb/0.7.0

if [ $# != 2 ]; then
	echo "Usage: $0 [Value size] [Workload]"
	exit 1
fi

ID=$(hostname | sed 's/testbed-node//g')

# Evenly distribute the # of ops to YCSB clients ( 8 in the experiment setting )
FIELD_LENGTH=$1
RECORD_COUNT=5000000
OPERATION_COUNT=$(expr ${RECORD_COUNT} \/ 4)
EXTRA_OP="-p fieldlength=${FIELD_LENGTH} -p table=t"

# Run the target workload
${YCSB_PATH}/bin/ycsb \
	run memec \
	-s \
	-P ${YCSB_PATH}/workloads/$2 \
	-p fieldcount=1 \
	-p readallfields=false \
	-p scanproportion=0 \
	-p requestdistribution=zipfian \
	-p recordcount=${RECORD_COUNT} \
	-p insertstart=${INSERT_START} \
	-p insertcount=${INSERT_COUNT} \
	-p operationcount=${OPERATION_COUNT} \
	-p threadcount=64 \
	-p histogram.buckets=200000 \
	-p memec.host=$(hostname -I | xargs) \
	-p memec.port=9112 \
	-p memec.key_size=255 \
	-p memec.chunk_size=4096 \
	-p zeropadding=19 \
	${EXTRA_OP}
