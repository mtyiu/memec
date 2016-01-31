#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio

coding='raid0' # raid1 raid5 rdp cauchy rs evenodd'
threads=64 # '16 32 64 128 256 512 1000'
workloads='load workloada workloadb workloadc workloadf workloadd'

for iter in {1..10}; do
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

				for n in {1..4}; do
					ssh client "screen -S ycsb${n} -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/master/workloads-memcached.sh $c $t $w $(printf '\r')\"" &
				done

				pending=0
				for n in {1..4}; do
					if [ $n == 1 ]; then
						read -p "Pending: ${pending} / 4" -t 360
					else
						read -p "Pending: ${pending} / 4" -t 120
					fi
					pending=$(expr $pending + 1)
				done
			done

			# screen -S manage -p 0 -X stuff "$(printf '\r\r')"
			screen -S manage -p 0 -X stuff "${HOME}/hwchan/util/memcached/start-distributed stop$(printf '\r')"
			sleep 10

			for n in {1..4}; do
				mkdir -p ${BASE_PATH}/results/workloads/memcached/$iter/client-ycsb$n
				scp node$n:${BASE_PATH}/results/client-ycsb$n/workloads/$c/$t/*.txt ${BASE_PATH}/results/workloads/memcached/$iter/client-ycsb$n
				ssh node$n 'rm -rf ${BASE_PATH}/results/client-ycsb${n}/*'
			done

			echo "Finished experiment with coding scheme = $c and thread count = $t..."
		done
	done
done
