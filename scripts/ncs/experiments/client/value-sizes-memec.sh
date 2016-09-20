#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec
HOSTNAME=$(hostname)
CONTROL_NODE=testbed-node10

c=$1 # number of clients
v=$2 # value size
w=$3 # workload

mkdir -p ${BASE_PATH}/results/value-size/$c/$v

echo "Running experiment with $c clients, value size = $v bytes..."

# Run workload A, B, C, F, D first
if [ $w == "load" ]; then
	echo "-------------------- Load (workloada) --------------------"
	${BASE_PATH}/scripts/ycsb/memec/load-value-$c.sh $v 2>&1 | tee ${BASE_PATH}/results/value-size/$c/$v/load.txt
else
	echo "-------------------- Run ($w) --------------------"
	${BASE_PATH}/scripts/ycsb/memec/run-value-$c.sh $v $w 2>&1 | tee ${BASE_PATH}/results/value-size/$c/$v/$w.txt
fi

# Tell the control node that this iteration is finished
ssh testbed-node10 "screen -S experiment -p 0 -X stuff \"$(printf '\r')\""

echo "Finished experiment with $c clients, value size = $v bytes..."
