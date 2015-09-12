#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.3.0

${YCSB_PATH}/bin/ycsb \
	load redis \
	-s \
	-P ${YCSB_PATH}/workloads/workloada \
	-p fieldcount=1 \
	-p readallfields=true \
	-p scanproportion=0 \
	-p fieldlength=100 \
	-p recordcount=1000000 \
	-p threadcount=8 \
	-p redis.host=137.189.88.46 \
	-p redis.port=6379
