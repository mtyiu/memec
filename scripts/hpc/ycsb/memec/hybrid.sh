#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.10.0

if [ $# != 1 ]; then
	echo "Usage: $0 [Number of threads]"
	exit 1
fi

${YCSB_PATH}/bin/ycsb \
	run memec \
	-s \
	-P ${YCSB_PATH}/workloads/workloada \
	-p fieldcount=1 \
	-p readallfields=false \
	-p scanproportion=0 \
	-p readproportion=0.34 \
	-p updateproportion=0.33 \
	-p insertproportion=0.33 \
	-p fieldlength=100 \
	-p recordcount=500000 \
	-p insertstart=0 \
	-p insertcount=500000 \
	-p operationcount=500000 \
	-p threadcount=$1 \
	-p memec.host=137.189.88.46 \
	-p memec.port=9112 \
	-p memec.key_size=255 \
	-p memec.chunk_size=4096
