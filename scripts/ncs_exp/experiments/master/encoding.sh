#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio
CONTROL_NODE=testbed-node10

c=$1 # coding
t=$2 # threads

mkdir -p ${BASE_PATH}/results/encoding/$c

echo "Running experiment with coding scheme = $c and thread count = $t..."
${BASE_PATH}/scripts/ycsb/plio/load.sh $t 2>&1 | tee ${BASE_PATH}/results/encoding/$c/$t.txt

# Tell the control node that this iteration is finished
ssh testbed-node10 "screen -S control -p 0 -X stuff \"$(printf '\r')\""

echo "Finished experiment with coding scheme = $c and thread count = $t..."
