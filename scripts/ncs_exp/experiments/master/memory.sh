#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio

cd ${PLIO_PATH}/test/memory

w=$1

mkdir -p ${BASE_PATH}/results/memory

echo "Running experiment for Workload #$w..."
time java -cp . edu.cuhk.cse.plio.Main 255 4096 $(hostname -I | xargs) 9112 $w 256 2000000000 2>&1 | tee ${BASE_PATH}/results/memory/$w.out

# Tell the control node that this iteration is finished
ssh testbed-node10 "screen -S experiment -p 0 -X stuff \"$(printf '\r')\""

echo "Finished experiment for Workload #$w..."
