#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio

coding=raid0
threads=64
workload=load

function set_overload {
	bd=10
	for n in 23; do
		echo "Lowering the bandwidth of node $n to $bd Mbps"
		ssh testbed-node$n "sudo ethtool -s eth0 speed $bd duplex full"
	done
}

function restore_overload {
	obd=1000
	for n in 23; do
		echo "Restoring the bandwidth of node $n to $obd Mbps"
		ssh testbed-node$n "sudo ethtool -s eth0 speed $obd duplex full"
	done
}

function set_config {
	sed -i "s/^smoothingFactor=.*$/smoothingFactor=$2/g" ${PLIO_PATH}/bin/config/ncs_exp/global.ini
	sed -i "s/^updateInterval=.*$/updateInterval=$1/g" ${PLIO_PATH}/bin/config/ncs_exp/coordinator.ini
	sed -i "s/^updateInterval=.*$/updateInterval=$1/g" ${PLIO_PATH}/bin/config/ncs_exp/master.ini
}

set_overload

for smoothing_factor in 0.1 0.2 0.3 0.4 0.5; do
	for stat_exchange_freq in 10 50 100 500 1000; do
		set_config $smoothing_factor $stat_exchange_freq
		${BASE_PATH}/scripts/util/rsync.sh

		for iter in {1..5}; do
			echo "Preparing for the experiment (smoothing factor = ${smoothing_factor}, statistics exchange frequency = ${stat_exchange_freq})..."

			screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts/util/start.sh $1$(printf '\r')"
			sleep 30

			for n in 3 4 8 9; do
				ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/master/workloads-memec.sh $coding $threads $workload $(printf '\r')\"" &
			done

			read -p "Press Enter when completed..."

			for n in 3 4 8 9; do
				ssh testbed-node$n "killall -9 ycsb"
			done

			echo "Done"

			screen -S manage -p 0 -X stuff "$(printf '\r\r')"
			sleep 10
		done
	done
done

restore_overload
