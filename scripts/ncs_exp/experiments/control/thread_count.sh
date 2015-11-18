#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio

threads='2 4 6 8 10 12 16 24 32 48 64 96 128 192 256 400 512 1000'

for n in 3 4 8 9; do
	ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/master/thread_count.sh $1$(printf '\r')\"" &
done

for t in $threads; do
	echo "Running experiment with thread count = $t..."
	screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts/util/start.sh $1$(printf '\r')"
	sleep 10

	mkdir -p ${BASE_PATH}/results/thread_count/$t

	for n in 3 4 8 9; do
		ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"$(printf '\r')\"" &
	done

	pending=0
	for n in 3 4 8 9; do
		read -p "Pending: ${pending} / 4"
		pending=$(expr $pending + 1)
	done

	echo "Finished experiment with thread count = $t..."

	screen -S manage -p 0 -X stuff "$(printf '\r\r')"

	for n in 3 4 8 9; do
		scp testbed-node$n:${BASE_PATH}/results/thread_count/$t.txt ${BASE_PATH}/results/thread_count/$t/node$n.txt
	done
done
