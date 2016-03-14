#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec

function set_overload {
	delay=$1
	variation=$(echo "scale = 1; $delay / 2" | bc | awk '{printf "%2.1f", $0}')
	for n in 11 23; do
		echo "Adding $delay +- $variation ms network delay to node $n..."
		ssh testbed-node$n "screen -S ethtool -p 0 -X stuff \"sudo tc qdisc add dev eth0 root netem delay ${delay}ms ${variation}ms distribution normal $(printf '\r')\""
		sleep 10
	done
}

function restore_overload {
	for n in 11 23; do
		echo "Removing the network delay from node $n"
		ssh testbed-node$n "screen -S ethtool -p 0 -X stuff \"sudo tc qdisc del root dev eth0 $(printf '\r')\""
		sleep 10
	done
}

workloads='workloada workloadc'
delays='0.4 0.8 1.2 1.6 2.0'

for delay in $delays; do
	for iter in {1..10}; do
		echo "******************** Iteration #$iter ********************"
		screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts/util/start.sh $(printf '\r')"
		sleep 30

		echo "-------------------- Load --------------------"
		for n in 3 4 8 9; do
			ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/client/degraded.sh load $(printf '\r')\"" &
		done

		pending=0
		for n in 3 4 8 9; do
			if [ $n == 3 ]; then
				read -p "Pending: ${pending} / 4" -t 300
			else
				read -p "Pending: ${pending} / 4" -t 60
			fi
			pending=$(expr $pending + 1)
		done

		set_overload $delay

		for w in $workloads; do
			echo "-------------------- Run ($w) --------------------"
			for n in 3 4 8 9; do
				ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/client/degraded.sh $w $(printf '\r')\"" &
			done

			pending=0
			for n in 3 4 8 9; do
				if [ $n == 3 ]; then
					read -p "Pending: ${pending} / 4" -t 300
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

		for n in 3 4 8 9; do
			mkdir -p ${BASE_PATH}/results/degraded_control/$delay/$iter/node$n
			scp testbed-node$n:${BASE_PATH}/results/degraded/*.txt ${BASE_PATH}/results/degraded_control/$delay/$iter/node$n
			ssh testbed-node$n 'rm -rf ${BASE_PATH}/results/*'
		done
	done
done
