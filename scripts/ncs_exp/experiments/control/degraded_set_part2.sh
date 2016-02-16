#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio
DELAYS=("400" "800" "1200" "1600" "2000")
IS_CONTROL=1

function set_manual {
	ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"manual$(printf '\r')\""
}

function set_slave {
	ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"overload$(printf '\r')7$(printf '\r')19$(printf '\r')0$(printf '\r')\""
}

function unset_slave {
	ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"underload$(printf '\r')7$(printf '\r')19$(printf '\r')0$(printf '\r')\""
}

function set_overload {
	for n in 11 23; do
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

## DELAY
for i in ${DELAYS[@]}; do
DELAY_BASE=$i 
echo ${DELAY_BASE}

if [ $IS_CONTROL -eq 1 ]; then
	OUT_NAME="exp4_control2"
else
	OUT_NAME="exp4_part2"
fi

#OUT_NAME="exp4_normal2"

DELAY_VAR="$(expr ${DELAY_BASE} \/ 2)us"
DELAY_BASE="${DELAY_BASE}us"

OUT_DIR=${BASE_PATH}/results/${OUT_NAME}/double/${DELAY_BASE}

workloads='workloadd'
#workloads=''

iter=$1
for iter in {1..10}; do
	echo "******************** Iteration #$iter (`date`) ********************"
	screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts/util/start.sh $(printf '\r')"
	sleep 30

	# no failure
	if [ $IS_CONTROL -ne 1 ]; then
		set_manual
	fi

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

	# failure after SET
	set_overload
	if [ $IS_CONTROL -ne 1 ]; then
		set_slave
	fi

	sleep 60

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

	echo "Done"

	# restore failure
	restore_overload

	screen -S manage -p 0 -X stuff "$(printf '\r\r')"
	sleep 30

	for n in 3 4 8 9; do
		mkdir -p ${OUT_DIR}/$iter/node$n
		scp testbed-node$n:${BASE_PATH}/results/degraded/*.txt ${OUT_DIR}/$iter/node$n
		ssh testbed-node$n 'rm -rf ${BASE_PATH}/results/*'
	done
done

#### DELAY
done
