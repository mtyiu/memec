#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.3.0

${YCSB_PATH}/bin/ycsb \
	run plio \
	-s \
	-P ${YCSB_PATH}/workloads/workloada \
	-p fieldcount=1 \
	-p readallfields=true \
	-p scanproportion=0 \
	-p fieldlength=100 \
	-p recordcount=1000000 \
	-p threadcount=4 \
	-p plio.host=192.168.0.19 \
	-p plio.port=9112 \
	-p plio.key_size=255 \
	-p plio.chunk_size=4096
