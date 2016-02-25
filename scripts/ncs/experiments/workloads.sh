#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec

coding='raid0 raid1 raid5 rdp cauchy rs evenodd'
threads='16 32 64 128 256 512 1000'
workloads='workloada workloadb workloadc workloadf workloadd'

for c in $coding; do
	echo "Preparing for the experiments with coding scheme = $c..."

	sed -i "s/^scheme=.*$/scheme=$c/g" ${MEMEC_PATH}/bin/config/ncs/global.ini

	${BASE_PATH}/scripts/util/rsync.sh

	mkdir -p ${BASE_PATH}/results/workloads/$c

	for t in $threads; do
		mkdir -p ${BASE_PATH}/results/workloads/$c/$t
		echo "Running experiment with coding scheme = $c and thread count = $t..."

		# Run workload A, B, C, F, D first
		screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts/util/start.sh $1$(printf '\r')"
		sleep 10

		echo "-------------------- Load (workloada) --------------------"
		${BASE_PATH}/scripts/ycsb/memec/load.sh $t 2>&1 | tee ${BASE_PATH}/results/workloads/$c/$t/load.txt
		for w in $workloads; do
			echo "-------------------- Run ($w) --------------------"
			${BASE_PATH}/scripts/ycsb/memec/run.sh $t $w 2>&1 | tee ${BASE_PATH}/results/workloads/$c/$t/$w.txt
		done

		screen -S manage -p 0 -X stuff "$(printf '\r\r')"
		sleep 10
		echo "Finished experiment with coding scheme = $c and thread count = $t..."
	done
done

sed -i "s/^scheme=.*$/scheme=raid0/g" ${MEMEC_PATH}/bin/config/ncs/global.ini

${BASE_PATH}/scripts/util/rsync.sh
