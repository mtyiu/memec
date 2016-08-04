#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.7.0

if [ $# != 1 ]; then
	echo "Usage: $0 [Number of threads]"
	exit 1
fi

FIELD_LENGTH=100
RECORD_COUNT=5000000

${YCSB_PATH}/bin/ycsb \
	load redis-cs \
	-s \
	-P ${YCSB_PATH}/workloads/workloada \
	-p fieldcount=1 \
	-p readallfields=false \
	-p scanproportion=0 \
	-p table=u \
	-p requestdistribution=zipfian \
	-p fieldlength=${FIELD_LENGTH} \
	-p recordcount=${RECORD_COUNT} \
	-p threadcount=$1 \
	-p redis.serverCount=8 \
	-p redis.host0=137.189.88.38 \
	-p redis.port0=6379 \
	-p redis.host1=137.189.88.39 \
	-p redis.port1=6379 \
	-p redis.host2=137.189.88.40 \
	-p redis.port2=6379 \
	-p redis.host3=137.189.88.41 \
	-p redis.port3=6379 \
	-p redis.host4=137.189.88.42 \
	-p redis.port4=6379 \
	-p redis.host5=137.189.88.43 \
	-p redis.port5=6379 \
	-p redis.host6=137.189.88.44 \
	-p redis.port6=6379 \
	-p redis.host7=137.189.88.45 \
	-p redis.port7=6379
