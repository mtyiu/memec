#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio
HOSTNAME=$(hostname)
CONTROL_NODE=testbed-node10

coding='raid0 raid1 raid5 rdp cauchy rs evenodd'
threads=64 # '16 32 64 128 256 512 1000'
workloads='workloada workloadb workloadc workloadf workloadd'

read -p "Press any key to start..."

for c in $coding; do
	echo "Preparing for the experiments with coding scheme = $c..."

	mkdir -p ${BASE_PATH}/results/workloads/$c

	for t in $threads; do
		mkdir -p ${BASE_PATH}/results/workloads/$c/$t
		echo "Running experiment with coding scheme = $c and thread count = $t..."

		# Run workload A, B, C, F, D first
		echo "-------------------- Load (workloada) --------------------"
		${BASE_PATH}/scripts/ycsb/plio/load.sh $t 2>&1 | tee ${BASE_PATH}/results/workloads/$c/$t/load.txt
		read -p "Press any key to continue..."
		# Tell the control node that this iteration is finished
		ssh testbed-node10 "screen -S control -p 0 -X stuff \"$(printf '\r')\""

		for w in $workloads; do
			echo "-------------------- Run ($w) --------------------"
			${BASE_PATH}/scripts/ycsb/plio/run.sh $t $w 2>&1 | tee ${BASE_PATH}/results/workloads/$c/$t/$w.txt
			read -p "Press any key to continue..."
			# Tell the control node that this iteration is finished
			ssh testbed-node10 "screen -S control -p 0 -X stuff \"$(printf '\r')\""
		done

		echo "Finished experiment with coding scheme = $c and thread count = $t..."

		read -p "Press any key to continue..."
	done
done

sed -i "s/^scheme=.*$/scheme=raid0/g" ${PLIO_PATH}/bin/config/ncs_exp/global.ini

${BASE_PATH}/scripts/util/rsync.sh
