#!/bin/bash

YCSB_PATH=~/mtyiu/ycsb/0.3.0

if [ $# != 1 ]; then
	echo "Usage: $0 [Number of threads]"
	exit 1
fi

ID=$(hostname | sed 's/testbed-node//g')
RECORD_COUNT=10000000
INSERT_COUNT=$(expr ${RECORD_COUNT} \/ 4)
if [ $ID == 3 ]; then
	INSERT_START=0
elif [ $ID == 4 ]; then
	INSERT_START=${INSERT_COUNT}
elif [ $ID == 8 ]; then
	INSERT_START=$(expr ${INSERT_COUNT} \* 2)
elif [ $ID == 9 ]; then
	INSERT_START=$(expr ${INSERT_COUNT} \* 3)
fi

${YCSB_PATH}/bin/ycsb \
	load plio_direct \
	-s \
	-P ${YCSB_PATH}/workloads/workloada \
	-p fieldcount=1 \
	-p readallfields=false \
	-p scanproportion=0 \
	-p table=u \
	-p fieldlength=200 \
	-p requestdistribution=zipfian \
	-p recordcount=${RECORD_COUNT} \
	-p insertstart=${INSERT_START} \
	-p insertcount=${INSERT_COUNT} \
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
