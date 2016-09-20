#!/bin/bash

###################################################
#
# Load key-value pairs using YCSB client
# INPUT: (1) Number of threads to use in each client, (2) name of the YCSB workload
#
###################################################

YCSB_PATH=~/mtyiu/ycsb/0.7.0

if [ $# != 1 ]; then
	echo "Usage: $0 [Value size]"
	exit 1
fi

ID=$(hostname | sed 's/testbed-node//g')

# Evenly distribute the # of ops to YCSB clients ( 8 in the experiment setting )
# Each client inserts pairs of an independent key range
FIELD_LENGTH=$1
RECORD_COUNT=5000000
INSERT_COUNT=$(expr ${RECORD_COUNT} \/ 4)
EXTRA_OP="-p fieldlength=${FIELD_LENGTH} -p table=t"
INSERT_START=$(echo "${INSERT_COUNT} * (${ID} - 2)" | bc -l)

# Load the store with key-value pairs
${YCSB_PATH}/bin/ycsb \
	load memec \
	-s \
	-P ${YCSB_PATH}/workloads/workloada \
	-p fieldcount=1 \
	-p readallfields=false \
	-p scanproportion=0 \
	-p requestdistribution=zipfian \
	-p recordcount=${RECORD_COUNT} \
	-p insertstart=${INSERT_START} \
	-p insertcount=${INSERT_COUNT} \
	-p threadcount=64 \
	-p histogram.buckets=200000 \
	-p memec.host=$(hostname -I | xargs) \
	-p memec.port=9112 \
	-p memec.key_size=255 \
	-p memec.chunk_size=4096 \
	-p zeropadding=19 \
	${EXTRA_OP}
