#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec

function set_overload {
	delay=$1
	variation=$(echo "scale = 1; $delay / 2" | bc | awk '{printf "%2.1f", $0}')
	for n in 11; do
		echo "Adding $delay +- $variation ms network delay to node $n..."
		ssh testbed-node$n "screen -S ethtool -p 0 -X stuff \"sudo tc qdisc add dev eth0 root netem delay ${delay}ms ${variation}ms distribution normal $(printf '\r')\""
		sleep 3
	done
}

function restore_overload {
	for n in 11; do
		echo "Removing the network delay from node $n"
		ssh testbed-node$n "screen -S ethtool -p 0 -X stuff \"sudo tc qdisc del root dev eth0 $(printf '\r')\""
		sleep 3
	done
}

workloads='workloada'
#workloads='updateonly'
#workloads=''
delays='2.0'

for delay in $delays; do
	for iter in {1..10}; do
		echo "******************** Iteration #$iter ********************"
		screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts/util/start.sh $(printf '\r')"
		sleep 30

		## overload ( before SET )
		#sleep 5
		#set_overload $delay
		#ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"overload$(printf '\r')7$(printf '\r')0$(printf '\r')\""
		#ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"overload$(printf '\r')7$(printf '\r')18$(printf '\r')0$(printf '\r')\""
		#sleep 3
		#ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"remapping$(printf '\r')\""

		echo "-------------------- Load --------------------"
		for n in 3 4 8 9; do
			ssh testbed-node$n "rm -rf ${BASE_PATH}/results/*"
			ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/client/transition.sh load $(printf '\r')\"" &
		done

		## overload ( during SET )
		#sleep 5
		#set_overload $delay
		#ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"overload$(printf '\r')7$(printf '\r')0$(printf '\r')\""
		#sleep 5
		#restore_overload
		#ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"remapping$(printf '\r')underload$(printf '\r')7$(printf '\r')0$(printf '\r')\""
		#sleep 3
		#ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"remapping$(printf '\r')\""

		pending=0
		for n in 3 4 8 9; do
			if [ $n == 3 ]; then
				read -p "Pending: ${pending} / 4" -t 300
			else
				read -p "Pending: ${pending} / 4" -t 60
			fi
			pending=$(expr $pending + 1)
		done

		### overload ( before workload )
		#sleep 5
		#set_overload $delay
		#ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"overload$(printf '\r')7$(printf '\r')0$(printf '\r')\""
		#sleep 3
		#ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"remapping$(printf '\r')\""
		#sleep 5

		for w in $workloads; do
			echo "-------------------- Run ($w) --------------------"
			for n in 3 4 8 9; do
				ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/client/transition.sh $w$(printf '\r')\"" &
			done

			### overload ( during workload )
			date
			sleep 15
			set_overload $delay
			date
			ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"overload$(printf '\r')7$(printf '\r')0$(printf '\r')\""
			date
			sleep 5
			restore_overload
			date
			ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"remapping$(printf '\r')underload$(printf '\r')7$(printf '\r')0$(printf '\r')\""
			date
			sleep 3
			ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"remapping$(printf '\r')\""

			pending=0
			for n in 3 4 8 9; do
				if [ $n == 3 ]; then
					read -p "Pending: ${pending} / 4" -t 90
				else
					read -p "Pending: ${pending} / 4" -t 20
				fi
				pending=$(expr $pending + 1)
			done
		done
		ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"remapping$(printf '\r')log$(printf '\r')\""

		echo "Done at $(date)"

		restore_overload

		screen -S manage -p 0 -X stuff "$(printf '\r\r')"
		sleep 10

		for n in 3 4 8 9; do
			mkdir -p ${BASE_PATH}/results/transit-single/$delay/$iter/node$n
			scp testbed-node$n:${BASE_PATH}/results/transition/*.txt ${BASE_PATH}/results/transit-single/$delay/$iter/node$n
			ssh testbed-node$n "rm -rf ${BASE_PATH}/results/*"
		done
		scp testbed-node1:${MEMEC_PATH}/coordinator.log ${BASE_PATH}/results/transit-single/$delay/$iter/coordinator.log
	done
done
