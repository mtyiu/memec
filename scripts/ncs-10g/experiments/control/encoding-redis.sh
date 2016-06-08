#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec
HOSTNAME=$(hostname)

threads=64 # '16 32 64 128 256 512 1000'
workloads='load workloada workloadc'
c=raid0

for iter in {1..10}; do
	mkdir -p ${BASE_PATH}/results/encoding/redis/$iter

	for t in $threads; do
		echo "Running experiment with Redis (3-way replication) and thread count = $t..."

		# Run workload A, B, C, F, D first
		# screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts/util/start.sh $1$(printf '\r')"
		screen -S manage -p 0 -X stuff "${HOME}/hwchan/util/redis/create-cluster-distributed-3-way start$(printf '\r')"
		sleep 10
		screen -S manage -p 0 -X stuff "${HOME}/hwchan/util/redis/create-cluster-distributed-3-way create$(printf '\r')"
		sleep 5
		screen -S manage -p 0 -X stuff "yes$(printf '\r')"
		sleep 10

		for w in $workloads; do
			if [ $w == "load" ]; then
				echo "-------------------- Load --------------------"
			else
				echo "-------------------- Run ($w) --------------------"
			fi

			for n in {31..34}; do
				ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/client/workloads-redis.sh $c $t $w $(printf '\r')\"" &
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
		done

		# screen -S manage -p 0 -X stuff "$(printf '\r\r')"
		screen -S manage -p 0 -X stuff "${HOME}/hwchan/util/redis/create-cluster-distributed-3-way stop$(printf '\r')"
		sleep 5
		screen -S manage -p 0 -X stuff "${HOME}/hwchan/util/redis/create-cluster-distributed-3-way clean$(printf '\r')"
		sleep 10

		for n in {31..34}; do
			mkdir -p ${BASE_PATH}/results/encoding/redis/$iter/node$n
			scp testbed-node$n:${BASE_PATH}/results/workloads/$c/$t/*.txt ${BASE_PATH}/results/encoding/redis/$iter/node$n
			ssh testbed-node$n 'rm -rf ${BASE_PATH}/results/*'
		done

		echo "Finished experiment with coding scheme = $c and thread count = $t..."
	done
done
