#!/bin/bash

YCSB_PATH=~/Development/ycsb/0.3.0

if [ $# != 1 ]; then
	echo "Usage: $0 [Number of threads]"
	exit 1
fi

${YCSB_PATH}/bin/ycsb \
	load plio \
	-s \
	-P ${YCSB_PATH}/workloads/workloada \
	-p fieldcount=1 \
	-p readallfields=false \
	-p scanproportion=0 \
	-p fieldlength=100 \
	-p recordcount=100000 \
	-p threadcount=$1 \
	-p plio.host=127.0.0.1 \
	-p plio.port=10091 \
	-p plio.key_size=255 \
	-p plio.chunk_size=4096 \
	-p table=usertable
