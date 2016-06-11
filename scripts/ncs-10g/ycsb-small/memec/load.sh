#!/bin/bash

###################################################
#
# Load key-value pairs using YCSB client
# INPUT: (1) Number of threads to use in each client, (2) name of the YCSB workload
#
###################################################

YCSB_PATH=~/mtyiu/ycsb/0.7.0

if [ $# != 1 ]; then
	echo "Usage: $0 [Number of threads]"
	exit 1
fi

ID=$(hostname | sed 's/testbed-node//g')

# Evenly distribute the # of ops to YCSB clients ( 4 in the experiment setting )
# Each client inserts pairs of an independent key range
RECORD_COUNT=5000000
INSERT_COUNT=$(expr ${RECORD_COUNT} \/ 2)
if [ $ID == 31 ]; then
	INSERT_START=0
	EXTRA_OP="-p table=a -p fieldlength=8"
elif [ $ID == 32 ]; then
	INSERT_START=${INSERT_COUNT}
	EXTRA_OP="-p table=a -p fieldlength=8"
elif [ $ID == 33 ]; then
	INSERT_START=0
	EXTRA_OP="-p table=b -p fieldlength=32"
elif [ $ID == 34 ]; then
	INSERT_START=${INSERT_COUNT}
	EXTRA_OP="-p table=b -p fieldlength=32"
fi

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
	-p threadcount=$1 \
	-p histogram.buckets=200000 \
	-p memec.host=$(hostname -I | sed 's/^.*\(192\.168\.10\.[0-9]*\).*$/\1/g') \
	-p memec.port=9112 \
	-p memec.key_size=255 \
	-p memec.chunk_size=4096 \
	-p zeropadding=19 \
	${EXTRA_OP}
