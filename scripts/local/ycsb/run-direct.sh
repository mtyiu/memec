#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.3.0

if [ $# != 2 ]; then
	echo "Usage: $0 [Number of threads] [Workload]"
	exit 1
fi

${YCSB_PATH}/bin/ycsb \
	run plio_direct \
	-s \
	-P ${YCSB_PATH}/workloads/$2 \
	-p fieldcount=1 \
	-p readallfields=false \
	-p scanproportion=0 \
	-p fieldlength=100 \
	-p recordcount=100000 \
	-p operationcount=100000 \
	-p threadcount=$1 \
	-p table=usertable \
	-p plio.key_size=255 \
	-p plio.chunk_size=4096 \
	-p plio.server_count=8 \
	-p plio.host0=127.0.0.1 \
	-p plio.port0=9111 \
	-p plio.host1=127.0.0.1 \
	-p plio.port1=9112 \
	-p plio.host2=127.0.0.1 \
	-p plio.port2=9113 \
	-p plio.host3=127.0.0.1 \
	-p plio.port3=9114 \
	-p plio.host4=127.0.0.1 \
	-p plio.port4=9115 \
	-p plio.host5=127.0.0.1 \
	-p plio.port5=9116 \
	-p plio.host6=127.0.0.1 \
	-p plio.port6=9117 \
	-p plio.host7=127.0.0.1 \
	-p plio.port7=9118
