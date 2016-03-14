#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.3.0

FIELD_LENGTH=100
RECORD_COUNT=12000000 # 1.68 GB

if [ $# -lt 1 ]; then
	echo "Usage: $0 [Number of threads]"
	exit 1
fi

if [ $# == 1 ]; then
	${YCSB_PATH}/bin/ycsb \
		load memec \
		-s \
		-P ${YCSB_PATH}/workloads/workloada \
		-p fieldcount=1 \
		-p readallfields=false \
		-p scanproportion=0 \
		-p fieldlength=${FIELD_LENGTH} \
		-p recordcount=${RECORD_COUNT} \
		-p threadcount=$1 \
		-p memec.host=137.189.88.46 \
		-p memec.port=9112 \
		-p memec.key_size=255 \
		-p memec.chunk_size=4096 \
		-p table=usertable
elif [ $# == 2 ]; then
	INSERT_COUNT=$(expr ${RECORD_COUNT} \/ 6)
	${YCSB_PATH}/bin/ycsb \
		load memec \
		-s \
		-P ${YCSB_PATH}/workloads/workloada \
		-p fieldcount=1 \
		-p readallfields=false \
		-p scanproportion=0 \
		-p fieldlength=${FIELD_LENGTH} \
		-p recordcount=${RECORD_COUNT} \
		-p threadcount=$1 \
		-p insertstart=$(expr ${INSERT_COUNT} \* $2) \
		-p insertcount=${INSERT_COUNT} \
		-p memec.host=$(hostname -I | xargs) \
		-p memec.port=9112 \
		-p memec.key_size=255 \
		-p memec.chunk_size=4096 \
		-p table=usertable
fi
