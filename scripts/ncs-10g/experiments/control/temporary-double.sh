#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec

function set_overload {
	delay=$1
	variation=$(echo "scale = 1; $delay / 2" | bc | awk '{printf "%2.1f", $0}')
	for n in 11 19; do
		echo "Adding $delay +- $variation ms network delay to node $n..."
		ssh testbed-node$n "screen -S ethtool -p 0 -X stuff \"sudo tc qdisc add dev eth2 root netem delay ${delay}ms ${variation}ms distribution normal $(printf '\r')\""
		sleep 3
	done
}

function restore_overload {
	for n in 11 19; do
		echo "Removing the network delay from node $n"
		ssh testbed-node$n "screen -S ethtool -p 0 -X stuff \"sudo tc qdisc del root dev eth2 $(printf '\r')\""
		sleep 3
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
			ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${MEMEC_PATH}/scripts/ncs-10g/experiments/client/temporary.sh load $(printf '\r')\"" &
		done

		pending=0
		for n in {31..34}; do
			if [ $n == 31 ]; then
				read -p "Pending: ${pending} / 4" -t 300
			else
				read -p "Pending: ${pending} / 4" -t 60
			fi
			pending=$(expr $pending + 1)
		done

		ssh testbed-node30 "screen -S coordinator -p 0 -X stuff \"overload$(printf '\r')7$(printf '\r')15$(printf '\r')0$(printf '\r')\""
		set_overload $delay

		for w in $workloads; do
			echo "-------------------- Run ($w) --------------------"
			for n in {31..34}; do
				ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${MEMEC_PATH}/scripts/ncs-10g/experiments/client/temporary.sh $w $(printf '\r')\"" &
			done

			pending=0
			for n in {31..34}; do
				if [ $n == 31 ]; then
					read -p "Pending: ${pending} / 4" -t 90
				else
					read -p "Pending: ${pending} / 4" -t 10
				fi
				pending=$(expr $pending + 1)
			done
		done

		echo "Done"

		restore_overload

		screen -S manage -p 0 -X stuff "$(printf '\r\r')"
		sleep 10

		for n in {31..34}; do
			mkdir -p ${BASE_PATH}/results/temporary-double/$delay/$iter/node$n
			scp testbed-node$n:${BASE_PATH}/results/temporary/*.txt ${BASE_PATH}/results/temporary-double/$delay/$iter/node$n
			ssh testbed-node$n 'rm -rf ${BASE_PATH}/results/*'
		done
		scp testbed-node1:${MEMEC_PATH}/coordinator.log ${BASE_PATH}/results/temporary-double/$delay/$iter/node$n
	done
done
