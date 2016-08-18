#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec
HOSTNAME=$(hostname)

coding='raid0' # raid1 raid5 rdp cauchy rs evenodd'
threads=64 # '16 32 64 128 256 512 1000'
workloads='load workloada workloadb workloadc workloadf workloadd'

for iter in {1..10}; do
	TIMESTAMP=$(date +%Y%m%d-%H%M%S)

	mkdir -p ${BASE_PATH}/results-multi/workloads/memec/$TIMESTAMP

	for c in $coding; do
		echo "Preparing for the experiments with coding scheme = $c..."

		sed -i "s/^scheme=.*$/scheme=$c/g" ${MEMEC_PATH}/bin/config/hpc/global.ini

		for t in $threads; do
			echo "Running experiment with coding scheme = $c and thread count = $t..."

			# Run workload A, B, C, F, D first
			screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts-multi/util/start.sh $1$(printf '\r')"
			sleep 150

			for w in $workloads; do
				if [ $w == "load" ]; then
					echo "-------------------- Load --------------------"
				else
					echo "-------------------- Run ($w) --------------------"
				fi

				for n in {7..14}; do
					ssh hpc$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts-multi/experiments/client/workloads-memec.sh $c $t $w $(printf '\r')\"" &
				done

				pending=0
				for n in {7..14}; do
					if [ $n == 7 ]; then
						read -p "Pending: ${pending} / 8" -t 300
					else
						read -p "Pending: ${pending} / 8" -t 60
					fi
					pending=$(expr $pending + 1)
				done

				sleep 30
			done

			screen -S manage -p 0 -X stuff "$(printf '\r\r')"
			sleep 10

			for n in {7..14}; do
				mkdir -p ${BASE_PATH}/results-multi/workloads/memec/$TIMESTAMP/hpc$n
				cp ${BASE_PATH}/results-multi/workloads/tmp/hpc$n/$c/$t/*.txt ${BASE_PATH}/results-multi/workloads/memec/$TIMESTAMP/hpc$n
				rm -rf ${BASE_PATH}/results-multi/workloads/tmp/hpc$n/*
			done

			echo "Finished experiment with coding scheme = $c and thread count = $t..."
		done
	done
done

sed -i "s/^scheme=.*$/scheme=raid0/g" ${MEMEC_PATH}/bin/config/hpc/global.ini
