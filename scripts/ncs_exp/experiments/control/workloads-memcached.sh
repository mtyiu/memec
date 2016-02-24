#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio
HOSTNAME=$(hostname)

coding='raid0' # raid1 raid5 rdp cauchy rs evenodd'
threads=64 # '16 32 64 128 256 512 1000'
workloads='load workloada workloadb workloadc workloadf workloadd'

for iter in {1..30}; do
	mkdir -p ${BASE_PATH}/results/workloads/memcached/$iter

	for c in $coding; do
		echo "Preparing for the experiments with coding scheme = $c..."

		for t in $threads; do
			echo "Running experiment with coding scheme = $c and thread count = $t..."

			# Run workload A, B, C, F, D first
			# screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts/util/start.sh $1$(printf '\r')"
			screen -S manage -p 0 -X stuff "${HOME}/hwchan/util/memcached/start-distributed start$(printf '\r')"
			sleep 30

			for w in $workloads; do
				if [ $w == "load" ]; then
					echo "-------------------- Load --------------------"
				else
					echo "-------------------- Run ($w) --------------------"
				fi

				for n in 3 4 8 9; do
					ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/client/workloads-memcached.sh $c $t $w $(printf '\r')\"" &
				done

				pending=0
				for n in 3 4 8 9; do
					read -p "Pending: ${pending} / 4"
					pending=$(expr $pending + 1)
				done
			done

			# screen -S manage -p 0 -X stuff "$(printf '\r\r')"
			screen -S manage -p 0 -X stuff "${HOME}/hwchan/util/memcached/start-distributed stop$(printf '\r')"
			sleep 10

			for n in 3 4 8 9; do
				mkdir -p ${BASE_PATH}/results/workloads/memcached/$iter/node$n
				scp testbed-node$n:${BASE_PATH}/results/workloads/$c/$t/*.txt ${BASE_PATH}/results/workloads/memcached/$iter/node$n
				ssh testbed-node$n 'rm -rf ${BASE_PATH}/results/*'
			done

			echo "Finished experiment with coding scheme = $c and thread count = $t..."
		done
	done
done
