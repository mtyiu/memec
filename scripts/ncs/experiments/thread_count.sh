#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio

threads=1000 #'2 4 6 8 10 12 16 24 32 48 64 96 128 192 256 400 512 1000'

mkdir -p ${BASE_PATH}/results/thread_count

for t in $threads; do
	echo "Running experiment with thread count = $t..."
	screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts/util/start.sh $1$(printf '\r')"
	sleep 10
	${BASE_PATH}/scripts/ycsb/plio/load.sh $t 2>&1 | tee ${BASE_PATH}/results/thread_count/$t.txt
	screen -S manage -p 0 -X stuff "$(printf '\r\r')"
	sleep 10
	echo "Finished experiment with thread count = $t..."
done
