#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.3.0

if [ $# != 2 ]; then
	echo "Usage: $0 [Number of threads] [Workload]"
	exit 1
fi

RECORD_COUNT=10000000
OPERATION_COUNT=$(expr ${RECORD_COUNT} \/ 4)

${YCSB_PATH}/bin/ycsb \
	run plio_direct \
	-s \
	-P ${YCSB_PATH}/workloads/$2 \
	-p fieldcount=1 \
	-p readallfields=false \
	-p scanproportion=0 \
	-p table=u \
	-p fieldlength=200 \
	-p requestdistribution=zipfian \
	-p recordcount=${RECORD_COUNT} \
	-p operationcount=${OPERATION_COUNT} \
	-p threadcount=$1 \
	-p histogram.buckets=200000 \
	-p plio.key_size=255 \
	-p plio.chunk_size=4096 \
	-p plio.server_count=16 \
	-p plio.host0=192.168.0.21 \
	-p plio.port0=9111 \
	-p plio.host1=192.168.0.22 \
	-p plio.port1=9111 \
	-p plio.host2=192.168.0.23 \
	-p plio.port2=9111 \
	-p plio.host3=192.168.0.24 \
	-p plio.port3=9111 \
	-p plio.host4=192.168.0.25 \
	-p plio.port4=9111 \
	-p plio.host5=192.168.0.26 \
	-p plio.port5=9111 \
	-p plio.host6=192.168.0.27 \
	-p plio.port6=9111 \
	-p plio.host7=192.168.0.28 \
	-p plio.port7=9111 \
	-p plio.host8=192.168.0.29 \
	-p plio.port8=9111 \
	-p plio.host9=192.168.0.30 \
	-p plio.port9=9111 \
	-p plio.host10=192.168.0.31 \
	-p plio.port10=9111 \
	-p plio.host11=192.168.0.32 \
	-p plio.port11=9111 \
	-p plio.host12=192.168.0.33 \
	-p plio.port12=9111 \
	-p plio.host13=192.168.0.47 \
	-p plio.port13=9111 \
	-p plio.host14=192.168.0.48 \
	-p plio.port14=9111 \
	-p plio.host15=192.168.0.49 \
	-p plio.port15=9111
