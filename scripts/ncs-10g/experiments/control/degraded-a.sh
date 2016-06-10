#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec

function set_overload {
	delay=$1
	variation=$(echo "scale = 1; $delay / 2" | bc | awk '{printf "%2.1f", $0}')
	for n in 11; do
		echo "Adding $delay +- $variation ms network delay to node $n..."
		ssh testbed-node$n "screen -S ethtool -p 0 -X stuff \"sudo tc qdisc add dev eth2 root netem delay ${delay}ms ${variation}ms distribution normal $(printf '\r')\""
		sleep 10
	done
}

function restore_overload {
	for n in 11; do
		echo "Removing the network delay from node $n"
		ssh testbed-node$n "screen -S ethtool -p 0 -X stuff \"sudo tc qdisc del root dev eth2 $(printf '\r')\""
		sleep 10
	done
}

workloads='workloada'
delays='2.0'

for delay in $delays; do
	for iter in {1..10}; do
		echo "******************** Iteration #$iter ********************"
		screen -S manage -p 0 -X stuff "${MEMEC_PATH}/scripts/ncs-10g/util/start.sh $(printf '\r')"
		sleep 30

		echo "-------------------- Load --------------------"
		for n in {31..34}; do
			ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${MEMEC_PATH}/scripts/ncs-10g/experiments/client/degraded.sh load $(printf '\r')\"" &
		done

		pending=0
		for n in {31..34}; do
			if [ $n == 31 ]; then
				read -p "Pending: ${pending} / 4" -t 60
			else
				read -p "Pending: ${pending} / 4" -t 120
			fi
			pending=$(expr $pending + 1)
		done

		ssh testbed-node30 "screen -S coordinator -p 0 -X stuff \"overload$(printf '\r')7$(printf '\r')0$(printf '\r')\""
		set_overload $delay

		for w in $workloads; do
			for n in {31..34}; do
				ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${MEMEC_PATH}/scripts/ncs-10g/experiments/client/degraded.sh $w $(printf '\r')\"" &
			done

			pending=0
			for n in {31..34}; do
				if [ $n == 31 ]; then
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

		for n in {31..34}; do
			mkdir -p ${BASE_PATH}/results/degraded-a/$delay/$iter/node$n
			scp testbed-node$n:${BASE_PATH}/results/degraded/*.txt ${BASE_PATH}/results/degraded-a/$delay/$iter/node$n
			ssh testbed-node$n 'rm -rf ${BASE_PATH}/results/*'
		done
	done
done
