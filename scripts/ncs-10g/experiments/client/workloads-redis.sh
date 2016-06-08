#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec
HOSTNAME=$(hostname)
CONTROL_NODE=testbed-node10

c=$1 # coding
t=$2 # threads
w=$3 # worklaod

mkdir -p ${BASE_PATH}/results/workloads/$c/$t

echo "Running experiment with coding scheme = $c and thread count = $t..."

# Run workload A, B, C, F, D first
if [ $w == "load" ]; then
	echo "-------------------- Load (workloada) --------------------"
	# ${BASE_PATH}/scripts/ycsb/memec/load.sh $t 2>&1 | tee ${BASE_PATH}/results/workloads/$c/$t/load.txt
	${BASE_PATH}/scripts/ycsb/redis/load.sh $t 2>&1 | tee ${BASE_PATH}/results/workloads/$c/$t/load.txt
else
	echo "-------------------- Run ($w) --------------------"
	# ${BASE_PATH}/scripts/ycsb/memec/run.sh $t $w 2>&1 | tee ${BASE_PATH}/results/workloads/$c/$t/$w.txt
	${BASE_PATH}/scripts/ycsb/redis/run.sh $t $w 2>&1 | tee ${BASE_PATH}/results/workloads/$c/$t/$w.txt
fi

# Tell the control node that this iteration is finished
ssh testbed-node10 "screen -S experiment -p 0 -X stuff \"$(printf '\r')\""

echo "Finished experiment with coding scheme = $c and thread count = $t..."
