#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio

function set_overload {
	for n in 11; do
		echo "Adding 0.2 +- 0.1 ms network delay to node $n..."
		ssh testbed-node$n "screen -S ethtool -p 0 -X stuff \"sudo tc qdisc add dev eth0 root netem delay 0.2ms 0.1ms distribution normal $(printf '\r')\""
		sleep 10
	done
}

function restore_overload {
	for n in 11; do
		echo "Removing the network delay from node $n"
		ssh testbed-node$n "screen -S ethtool -p 0 -X stuff \"sudo tc qdisc del root dev eth0 $(printf '\r')\""
		sleep 10
	done
}

workloads='workloadc workloada'

for iter in {1..1}; do
	echo "******************** Iteration #$iter ********************"
	screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts/util/start.sh $(printf '\r')"
	sleep 30

	echo "-------------------- Load --------------------"
	for n in 3 4 8 9; do
		ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/master/degraded.sh load $(printf '\r')\"" &
	done

	pending=0
	for n in 3 4 8 9; do
		if [ $n == 3 ]; then
			read -p "Pending: ${pending} / 4" -t 60
		else
			read -p "Pending: ${pending} / 4" -t 120
		fi
		pending=$(expr $pending + 1)
	done

	set_overload

	for w in $workloads; do
		for n in 3 4 8 9; do
			ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/master/degraded.sh $w $(printf '\r')\"" &
		done

		pending=0
		for n in 3 4 8 9; do
			if [ $n == 3 ]; then
				read -p "Pending: ${pending} / 4" -t 500
			else
				read -p "Pending: ${pending} / 4" -t 60
			fi
			pending=$(expr $pending + 1)
		done
	done

	restore_overload

	echo "Done"

	screen -S manage -p 0 -X stuff "$(printf '\r\r')"
	sleep 30

	for n in 3 4 8 9; do
		mkdir -p ${BASE_PATH}/results/degraded/$iter/node$n
		scp testbed-node$n:${BASE_PATH}/results/degraded/*.txt ${BASE_PATH}/results/degraded/$iter/node$n
		ssh testbed-node$n 'rm -rf ${BASE_PATH}/results/*'
	done
done
