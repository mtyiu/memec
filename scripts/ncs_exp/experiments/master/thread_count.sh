#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio
CONTROL_NODE=testbed-node10

threads='2 4 6 8 10 12 16 24 32 48 64 96 128 192 256 400 512 1000'

mkdir -p ${BASE_PATH}/results/thread_count

read -p "Press any key to start..."

for t in $threads; do
	echo "Running experiment with thread count = $t..."
	${BASE_PATH}/scripts/ycsb/plio/load.sh $t 2>&1 | tee ${BASE_PATH}/results/thread_count/$t.txt

	echo "Finished experiment with thread count = $t..."

	# Tell the control node that this iteration is finished
	ssh testbed-node10 "screen -S control -p 0 -X stuff \"$(printf '\r')\""

	read -p "Press any key to continue..."
done
