#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec

sizes='8 32 64 128 256 512 1024 2048 4040 4096 8192 16384'
workloads='load workloada workloadc'

for s in $sizes; do
	echo "Preparing for the experiments with size = $s..."

	for iter in {1..10}; do
		TIMESTAMP=$(date +%Y%m%d-%H%M%S)

		screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts-multi/util/start.sh $1$(printf '\r')"
		sleep 150

		for w in $workloads; do
			if [ $w == "load" ]; then
				echo "-------------------- Load --------------------"
			else
				echo "-------------------- Run ($w) --------------------"
			fi

			for n in {7..14}; do
				ssh hpc$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts-multi/experiments/client/value_sizes-memec.sh $s $w $(printf '\r')\"" &
			done

			pending=0
			echo "Start waiting at: $(date)..."
			for n in {7..14}; do
				if [ $n == 7 ]; then
					read -p "Pending: ${pending} / 8" -t 300
				else
					read -p "Pending: ${pending} / 8" -t 60
				fi
				pending=$(expr $pending + 1)
			done
			echo "Done at: $(date)."

			sleep 30
		done

		echo "Finished experiment with size = $s..."

		screen -S manage -p 0 -X stuff "$(printf '\r\r')"
		sleep 10

		for n in {7..14}; do
			mkdir -p ${BASE_PATH}/results-multi/value_sizes/memec/$TIMESTAMP/hpc$n
			cp ${BASE_PATH}/results-multi/value_sizes/tmp/hpc$n/$s/*.txt ${BASE_PATH}/results-multi/value_sizes/memec/$TIMESTAMP/hpc$n
			rm -rf ${BASE_PATH}/results-multi/value_sizes/tmp/hpc$n/*
		done
	done
done
