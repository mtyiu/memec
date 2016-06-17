#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec
CONTROL_NODE=testbed-node10

t=$1 # threads

mkdir -p ${BASE_PATH}/results/thread_count

echo "Running experiment with thread count = $t..."
${MEMEC_PATH}/scripts/ncs-10g/ycsb/memec/load.sh $t 2>&1 | tee ${BASE_PATH}/results/thread_count/$t.txt

# Tell the control node that this iteration is finished
ssh testbed-node10 "screen -S experiment -p 0 -X stuff \"$(printf '\r')\""

echo "Finished experiment with thread count = $t..."
