#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec
HOSTNAME=$(hostname | sed 's/.cse.cuhk.edu.hk//g')
CONTROL_NODE=hpc15

t=$1 # threads

mkdir -p ${BASE_PATH}/results-multi/thread_count/tmp/${HOSTNAME}

echo "Running experiment with thread count = $t..."
${BASE_PATH}/scripts-multi/ycsb/memec/load.sh $t 2>&1 | tee ${BASE_PATH}/results-multi/thread_count/tmp/${HOSTNAME}/$t.txt

# Tell the control node that this iteration is finished
ssh ${CONTROL_NODE} "screen -S experiment -p 0 -X stuff \"$(printf '\r')\""

echo "Finished experiment with thread count = $t..."
