#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio

function set_overload {
	delay=$1
	variation=$(echo "scale = 1; $delay / 2" | bc | awk '{printf "%2.1f", $0}')
	for n in 11 23; do
		echo "Adding $delay +- $variation ms network delay to node $n..."
		ssh testbed-node$n "screen -S ethtool -p 0 -X stuff \"sudo tc qdisc add dev eth0 root netem delay ${delay}ms ${variation}ms distribution normal $(printf '\r')\""
		sleep 3
	done
}

function restore_overload {
	for n in 11 23; do
		echo "Removing the network delay from node $n"
		ssh testbed-node$n "screen -S ethtool -p 0 -X stuff \"sudo tc qdisc del root dev eth0 $(printf '\r')\""
		sleep 3
	done
}

workloads='workloada workloadc'
workloads='workloadc'
delays='2.0'

for delay in $delays; do
	for iter in {1..10}; do
		echo "******************** Iteration #$iter ********************"
		screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts/util/start.sh $1$(printf '\r')"
		sleep 30

		echo "-------------------- Load --------------------"
		for n in 3 4 8 9; do
			ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/client/degraded.sh load $(printf '\r')\"" &
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

		ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"overload$(printf '\r')7$(printf '\r')19$(printf '\r')0$(printf '\r')\""
		set_overload $delay

		for w in $workloads; do
			for n in 3 4 8 9; do
				ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/client/degraded.sh $w $(printf '\r')\"" &
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
			mkdir -p ${BASE_PATH}/results/degraded-double-c/$delay/$iter/node$n
			scp testbed-node$n:${BASE_PATH}/results/degraded/*.txt ${BASE_PATH}/results/degraded-double-c/$delay/$iter/node$n
			ssh testbed-node$n 'rm -rf ${BASE_PATH}/results/*'
		done
	done
done
