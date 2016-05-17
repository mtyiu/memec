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
INSERT_COUNT=$(expr ${RECORD_COUNT} \/ 2)
OPERATION_COUNT=$(expr ${RECORD_COUNT} \* 2 \/ 2)
if [ $ID == 3 ]; then
	INSERT_START=0
	EXTRA_OP="-p table=a -p fieldlength=8"
elif [ $ID == 4 ]; then
	INSERT_START=${INSERT_COUNT}
	EXTRA_OP="-p table=a -p fieldlength=8"
elif [ $ID == 8 ]; then
	INSERT_START=0
	EXTRA_OP="-p table=b -p fieldlength=32"
elif [ $ID == 9 ]; then
	INSERT_START=${INSERT_COUNT}
	EXTRA_OP="-p table=b -p fieldlength=32"
fi

EXTRA=""
workload=$2
if [ "$2" == "updateonly" ]; then
	workload="workloadc"
	EXTRA="-p readproportion=0.0 -p updateproportion=1.0 -p insertproportion=0.0"
fi

EXTRA_OP="${EXTRA_OP} ${EXTRA}"

# Run the target workload
${YCSB_PATH}/bin/ycsb \
	run memec \
	-s \
	-P ${YCSB_PATH}/workloads/${workload} \
	-p fieldcount=1 \
	-p readallfields=false \
	-p scanproportion=0 \
	-p requestdistribution=zipfian \
	-p recordcount=${RECORD_COUNT} \
	-p insertstart=${INSERT_START} \
	-p insertcount=${INSERT_COUNT} \
	-p operationcount=${OPERATION_COUNT} \
	-p threadcount=$1 \
	-p histogram.buckets=200000 \
	-p memec.host=$(hostname -I | xargs) \
	-p memec.port=9112 \
	-p memec.key_size=255 \
	-p memec.chunk_size=4096 \
	-p measurementtype=timeseries \
	-p timeseries.granularity=500 \
	-p zeropadding=19 \
	${EXTRA_OP}
