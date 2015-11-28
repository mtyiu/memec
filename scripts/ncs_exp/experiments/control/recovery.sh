#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio

sizes=1000000000 2000000000 4000000000 8000000000 16000000000 32000000000

for s in $sizes; do
	for iter in {1..10}; do
		mkdir -p ${BASE_PATH}/results/recovery/$s/$iter

		echo "Preparing for the experiments for size = $s..."

		screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts/util/start.sh $1$(printf '\r')"
		sleep 30

		echo "Writing $s bytes of data to the system..."
		size=$(expr $s \/ 4)
		for n in 3 4 8 9; do
			ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/master/recovery.sh $size $(printf '\r')\"" &
		done

		pending=0
		echo "Start waiting at: $(date)..."
		for n in 3 4 8 9; do
			if [ $n == 3 ]; then
				read -p "Pending: ${pending} / 4" -t 300
			else
				read -p "Pending: ${pending} / 4" -t 60
			fi
			pending=$(expr $pending + 1)
		done
		echo "Done at: $(date)."

		ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"$(printf '\r\r')seal ${1}$(printf '\r\r')\""
		sleep 10

		n=$(expr $RANDOM % 13 + 11)
		echo "Killing node $n..."

		ssh testbed-node$n "screen -S coordinator -p 0 -X stuff \"$(printf '\r\r')memory ${1}$(printf '\r\r')\""
		sleep 1
		ssh testbed-node$n "screen -S coordinator -p 0 -X stuff \"$(printf '\r\r')exit ${1}$(printf '\r\r')\""

		sleep 20

		ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"$(printf '\r\r')log ${1}$(printf '\r\r')\""

		screen -S manage -p 0 -X stuff "$(printf '\r\r')"
		sleep 10

		scp testbed-node$n:${PLIO_PATH}/memory.log ${BASE_PATH}/results/recovery/$s/$iter/
		scp testbed-node1:${PLIO_PATH}/coordinator.log ${BASE_PATH}/results/recovery/$s/$iter/
	done
done
