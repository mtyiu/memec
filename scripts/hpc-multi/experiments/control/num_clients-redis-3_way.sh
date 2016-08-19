#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec
REDIS_PATH=${BASE_PATH}/redis

sizes='8 32 64 128 256 512 1024 2048 4040 4096 8192 16384'
clients='1 2 3 4 5 6 7 8'
workloads='load workloada workloadc'

for s in $sizes; do
	echo "Preparing for the experiments with size = $s..."

	for c in $clients; do
		for iter in {1..10}; do
			TIMESTAMP=$(date +%Y%m%d-%H%M%S)

			screen -S manage -p 0 -X stuff "${REDIS_PATH}/utils/create-cluster-distributed-3-way start$(printf '\r')"
			sleep 10
			screen -S manage -p 0 -X stuff "${REDIS_PATH}/utils/create-cluster-distributed-3-way create$(printf '\r')"
			sleep 5
			screen -S manage -p 0 -X stuff "yes$(printf '\r')"
			sleep 10

			for w in $workloads; do
				if [ $w == "load" ]; then
					echo "-------------------- Load --------------------"
				else
					echo "-------------------- Run ($w) --------------------"
				fi

				for i in $(seq $c); do
					n=$(expr $i + 6)
					ssh hpc$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts-multi/experiments/client/value_sizes-redis-3_way.sh $s $w $c $(printf '\r')\"" &
				done

				pending=0
				echo "Start waiting at: $(date)..."
				for n in $(seq $c); do
					if [ $n == 1 ]; then
						read -p "Pending: ${pending} / $c" -t 300
					else
						read -p "Pending: ${pending} / $c" -t 60
					fi
					pending=$(expr $pending + 1)
				done
				echo "Done at: $(date)."

				sleep 30
			done

			echo "Finished experiment with size = $s..."

			screen -S manage -p 0 -X stuff "${REDIS_PATH}/utils/create-cluster-distributed-3-way stop$(printf '\r')"
			sleep 5
			screen -S manage -p 0 -X stuff "${REDIS_PATH}/utils/create-cluster-distributed-3-way clean$(printf '\r')"
			sleep 10

			for i in $(seq $c); do
				n=$(expr $i + 6)
				mkdir -p ${BASE_PATH}/results-multi/num_clients/redis-3_way/$c/$s/$TIMESTAMP/hpc$n
				cp ${BASE_PATH}/results-multi/value_sizes/tmp/hpc$n/$s/*.txt ${BASE_PATH}/results-multi/num_clients/redis-3_way/$c/$s/$TIMESTAMP/hpc$n
				rm -rf ${BASE_PATH}/results-multi/value_sizes/tmp/hpc$n/*
			done
		done
	done
done
