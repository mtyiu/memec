#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec

threads='2 4 6 8 10 12 16 24 32 48 64 96 128 192 256 400 512 1000'

for t in $threads; do
	echo "Running experiment with thread count = $t..."
	screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts-multi/util/start.sh $1$(printf '\r')"
	sleep 150

	mkdir -p ${BASE_PATH}/results-multi/thread_count/$t

	for n in {7..14}; do
		ssh hpc$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts-multi/experiments/client/thread_count.sh $t$(printf '\r')\"" &
	done

	pending=0
	for n in {7..14}; do
		read -p "Pending: ${pending} / 8"
		pending=$(expr $pending + 1)
	done

	echo "Finished experiment with thread count = $t..."

	screen -S manage -p 0 -X stuff "$(printf '\r\r')"
	sleep 10

	for n in {7..14}; do
		cp ${BASE_PATH}/results-multi/thread_count/tmp/hpc$n/$t.txt ${BASE_PATH}/results-multi/thread_count/$t/hpc$n.txt
		rm -rf ${BASE_PATH}/results-multi/thread_count/tmp/hpc$n
	done
done
