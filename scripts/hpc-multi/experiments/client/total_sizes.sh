#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec
HOSTNAME=$(hostname | sed 's/.cse.cuhk.edu.hk//g')
CONTROL_NODE=hpc15

s=$1

mkdir -p ${BASE_PATH}/results-multi/total_sizes/tmp/${HOSTNAME}/$s

echo "Running experiment with total size = $s..."

${BASE_PATH}/scripts-multi/ycsb/memec/load-total.sh $s 2>&1 | tee ${BASE_PATH}/results-multi/total_sizes/tmp/${HOSTNAME}/$s/load.txt

# Tell the control node that this iteration is finished
ssh ${CONTROL_NODE} "screen -S experiment -p 0 -X stuff \"$(printf '\r')\""

echo "Finished experiment with total size = $s."
