#!/bin/bash

echo "NOT YET MODIFIED!"

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio

function set_overload {
	for n in 11; do
		echo "Adding 0.4 +- 0.2 ms network delay to node $n..."
		ssh node$n "screen -S ethtool -p 0 -X stuff \"sudo tc qdisc add dev eth0 root netem delay 1ms 0.2ms distribution normal $(printf '\r')\""
		sleep 10
	done
}

function restore_overload {
	for n in 11; do
		echo "Removing the network delay from node $n"
		ssh node$n "screen -S ethtool -p 0 -X stuff \"sudo tc qdisc del root dev eth0 $(printf '\r')\""
		sleep 10
	done
}

workloads='workloadd'

iter=$1
# for iter in {1..30}; do
	echo "******************** Iteration #$iter ********************"
	screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts/util/start.sh $(printf '\r')"
	sleep 30

	echo "-------------------- Load --------------------"
	for n in {1..4}; do
		ssh node$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/client/degraded.sh load $(printf '\r')\"" &
	done

	pending=0
	for n in {1..4}; do
		if [ $n == 1 ]; then
			read -p "Pending: ${pending} / 4" -t 60
		else
			read -p "Pending: ${pending} / 4" -t 120
		fi
		pending=$(expr $pending + 1)
	done

	set_overload

	for w in $workloads; do
		for n in {1..4}; do
			ssh node$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/client/degraded.sh $w $(printf '\r')\"" &
		done

		pending=0
		for n in {1..4}; do
			if [ $n == 1 ]; then
				read -p "Pending: ${pending} / 4" -t 500
			else
				read -p "Pending: ${pending} / 4" -t 60
			fi
			pending=$(expr $pending + 1)
		done
	done

	echo "Done"

	restore_overload

	screen -S manage -p 0 -X stuff "$(printf '\r\r')"
	sleep 30

	for n in {1..4}; do
		mkdir -p ${BASE_PATH}/results/exp4_control2/$iter/node$n
		scp node$n:${BASE_PATH}/results/degraded/*.txt ${BASE_PATH}/results/exp4_control2/$iter/node$n
		ssh node$n 'rm -rf ${BASE_PATH}/results/*'
	done
# done
