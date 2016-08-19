#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec
HOSTNAME=$(hostname | sed 's/.cse.cuhk.edu.hk//g')
CONTROL_NODE=hpc15

s=$1
w=$2
c=$3

mkdir -p ${BASE_PATH}/results-multi/value_sizes/tmp/${HOSTNAME}/$s

echo "Running experiment with size = $s for workload: $w..."

# Run workload A, B, C, F, D first
if [ $w == "load" ]; then
	echo "-------------------- Load (workloada) --------------------"
	${BASE_PATH}/scripts-multi/ycsb/redis/load-size.sh $s $c 2>&1 | tee ${BASE_PATH}/results-multi/value_sizes/tmp/${HOSTNAME}/$s/load.txt
else
	echo "-------------------- Run ($w) --------------------"
	${BASE_PATH}/scripts-multi/ycsb/redis/run-size.sh $s $w $c 2>&1 | tee ${BASE_PATH}/results-multi/value_sizes/tmp/${HOSTNAME}/$s/$w.txt
fi

# Tell the control node that this iteration is finished
ssh ${CONTROL_NODE} "screen -S experiment -p 0 -X stuff \"$(printf '\r')\""

echo "Finished experiment with size = $s for workload: $w..."
