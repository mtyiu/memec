#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio

function set_overload {
	delay=$1
	variation=$(echo "scale = 1; $delay / 2" | bc | awk '{printf "%2.1f", $0}')
	for n in 1; do
	# for n in 1 10; do
		echo "Adding $delay +- $variation ms network delay to node $n..."
		ssh node$n "screen -S ethtool -p 0 -X stuff \"sudo tc qdisc add dev eth0 root netem delay ${delay}ms ${variation}ms distribution normal $(printf '\r')\""
		sleep 10
	done
}

function restore_overload {
	for n in 1; do
	# for n in 1 10; do
		echo "Removing the network delay from node $n"
		ssh node$n "screen -S ethtool -p 0 -X stuff \"sudo tc qdisc del root dev eth0 $(printf '\r')\""
		sleep 10
	done
}

workloads='workloada workloadc'
delays='0.4 0.8 1.2 1.6'

for delay in $delays; do
	for iter in {1..10}; do
		echo "******************** Iteration #$iter ********************"
		screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts/util/start.sh $(printf '\r')"
		sleep 30

		echo "-------------------- Load --------------------"
		for n in {1..4}; do
			ssh client "screen -S ycsb${n} -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/master/degraded.sh load $(printf '\r')\"" &
		done

		pending=0
		for n in {1..4}; do
			if [ $n == 1 ]; then
				read -p "Pending: ${pending} / 4" -t 300
			else
				read -p "Pending: ${pending} / 4" -t 60
			fi
			pending=$(expr $pending + 1)
		done

		set_overload $delay

		for w in $workloads; do
			echo "-------------------- Run ($w) --------------------"
			for n in {1..4}; do
				ssh client "screen -S ycsb${n} -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/master/degraded.sh $w $(printf '\r')\"" &
			done

			pending=0
			for n in {1..4}; do
				if [ $n == 1 ]; then
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

		for n in {1..4}; do
			mkdir -p ${BASE_PATH}/results/degraded_control/$delay/$iter/client-ycsb$n
			scp client:${BASE_PATH}/results/client-ycsb${n}/degraded/*.txt ${BASE_PATH}/results/degraded_control/$delay/$iter/client-ycsb${n}
			ssh client 'rm -rf ${BASE_PATH}/results/client-ycsb${n}/degraded/*'
		done
	done
done
