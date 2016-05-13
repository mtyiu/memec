#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec
HOSTNAME=$(hostname)
CONTROL_NODE=testbed-node10

t=64
w=$1 # worklaod

mkdir -p ${BASE_PATH}/results/transition

if [ $w == "load" ]; then
	echo "-------------------- Load (workloada) --------------------"
	${BASE_PATH}/scripts/ycsb/memec/load-timeseries.sh $t 2>&1 | tee ${BASE_PATH}/results/transition/load.txt
elif [ $w == "hybrid" ]; then
	echo "-------------------- Run ($w) --------------------"
	${BASE_PATH}/scripts/ycsb/memec/run-hybrid-timeseries.sh $t $w 2>&1 | tee ${BASE_PATH}/results/transition/$w.txt
elif [ $w == "updateonly" ]; then
	echo "-------------------- Run ($w) --------------------"
	${BASE_PATH}/scripts/ycsb/memec/run-timeseries.sh $t $w 2>&1 | tee ${BASE_PATH}/results/transition/$w.txt
else
	echo "-------------------- Run ($w) --------------------"
	${BASE_PATH}/scripts/ycsb/memec/run-timeseries.sh $t $w 2>&1 | tee ${BASE_PATH}/results/transition/$w.txt
fi

# Tell the control node that this iteration is finished
ssh testbed-node10 "screen -S experiment -p 0 -X stuff \"$(printf '\r')\""
