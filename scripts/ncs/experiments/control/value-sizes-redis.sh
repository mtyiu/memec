#!/bin/bash

BASE_PATH=${HOME}/mtyiu
HOSTNAME=$(hostname)

clients='4 8'
value_sizes='8 32 128 256 512 1024 4096 16384'
workloads='load workloada workloadc'

for c in $clients; do
	for v in $value_sizes; do
		for iter in {1..10}; do
			mkdir -p ${BASE_PATH}/results/value-sizes/redis/$c/$v/$iter

			echo "Running experiment with $c clients, value size = $v..."

			# Start Redis
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

				for n in {2..5}; do
					ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/client/value-sizes-redis.sh $c $v $w $(printf '\r')\"" &
				done

				if [ $c == "8" ]; then
					for n in {6..9}; do
						ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/client/value-sizes-redis.sh $c $v $w $(printf '\r')\"" &
					done
				fi

				pending=0
				for n in {2..5}; do
					if [ $n == 2 ]; then
						read -p "Pending: ${pending} / $c" -t 300
					else
						read -p "Pending: ${pending} / $c" -t 60
					fi
					pending=$(expr $pending + 1)
				done
				if [ $c == "8" ]; then
					for n in {6..9}; do
						read -p "Pending: ${pending} / $c" -t 300
						pending=$(expr $pending + 1)
					done
				fi

				sleep 30
			done

			screen -S manage -p 0 -X stuff "$(printf '\r\r')"
			sleep 10

			for n in {2..5}; do
				mkdir -p ${BASE_PATH}/results/value-sizes/redis/$c/$v/$iter/node$n
				scp testbed-node$n:${BASE_PATH}/results/value-sizes/redis/$c/$v/*.txt ${BASE_PATH}/results/value-sizes/redis/$c/$v/$iter/node$n
				ssh testbed-node$n 'rm -rf ${BASE_PATH}/results/*'
			done
			if [ $c == "8" ]; then
				for n in {6..9}; do
					mkdir -p ${BASE_PATH}/results/value-sizes/redis/$c/$v/$iter/node$n
					scp testbed-node$n:${BASE_PATH}/results/value-sizes/redis/$c/$v/*.txt ${BASE_PATH}/results/value-sizes/redis/$c/$v/$iter/node$n
					ssh testbed-node$n 'rm -rf ${BASE_PATH}/results/*'
				done
			fi

			echo "Finished experiment with $c clients, value size = $v."
		done
	done
done
