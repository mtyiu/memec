#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio
DELAY_BASE="0.2ms"
DELAY_VAR="0.1ms"
DELAY=("2000")

function set_overload {
	for n in 11; do
		echo "Adding ${DELAY_BASE} +- ${DELAY_VAR} network delay to node $n..."
		ssh testbed-node$n "screen -S ethtool -p 0 -X stuff \"sudo tc qdisc add dev eth0 root netem delay ${DELAY_BASE} ${DELAY_VAR} distribution normal $(printf '\r')\""
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

function set_slave {
	ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"manual$(printf '\r')overload$(printf '\r')6$(printf '\r')21$(printf '\r')0$(printf '\r')\""
}

function unset_slave {
	ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"manual$(printf '\r')underload$(printf '\r')6$(printf '\r')21$(printf '\r')0$(printf '\r')\""
}

workloads='workloada'
#workloads=

for i in ${DELAY[@]}; do

DELAY_BASE=$i
DELAY_VAR="$(expr ${DELAY_BASE} \/ 2)us" 
DELAY_BASE="${i}us"
OUT_PATH="${BASE_PATH}/results/exp4_part1_double/${DELAY_BASE}/"

echo "$DELAY_BASE $DELAY_VAR"

set_overload

iter=$1
for iter in {1..10}; do
	echo "******************** Iteration #$iter ( `date` ) ********************"
	screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts/util/start.sh $(printf '\r')"
	sleep 30

	set_slave
	sleep 5

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

	sleep 60

	for w in $workloads; do
		for n in 3 4 8 9; do
			ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/master/degraded.sh $w $(printf '\r')\"" &
		done

		pending=0
		for n in 3 4 8 9; do
			if [ $n == 3 ]; then
				read -p "Pending: ${pending} / 4" -t 100
			else
				read -p "Pending: ${pending} / 4" -t 200
			fi
			pending=$(expr $pending + 1)
		done
	done


	echo "Done"

	screen -S manage -p 0 -X stuff "$(printf '\r\r')"
	sleep 30

	for n in 3 4 8 9; do
		mkdir -p ${OUT_PATH}/$iter/node$n
		scp testbed-node$n:${BASE_PATH}/results/degraded/*.txt ${OUT_PATH}/$iter/node$n
		ssh testbed-node$n 'rm -rf ${BASE_PATH}/results/*'
	done
done

restore_overload

done
