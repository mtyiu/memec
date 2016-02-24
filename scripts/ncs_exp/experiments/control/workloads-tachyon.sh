#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio
HOSTNAME=$(hostname)

coding='raid0' # raid1 raid5 rdp cauchy rs evenodd'
threads=64 # '16 32 64 128 256 512 1000'
workloads='load workloada workloadb workloadc workloadf workloadd'

for iter in {1..30}; do
	mkdir -p ${BASE_PATH}/results/workloads/tachyon/$iter

	for c in $coding; do
		echo "Preparing for the experiments with coding scheme = $c..."

		for t in $threads; do
			echo "Running experiment with coding scheme = $c and thread count = $t..."

			# Run workload A, B, C, F, D first
			ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"${HOME}/hwchan/tachyon/bin/tachyon format$(printf '\r')\""
			sleep 15
			ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"${HOME}/hwchan/tachyon/bin/tachyon-start.sh all SudoMount$(printf '\r')\""
			sleep 60

			for w in $workloads; do
				if [ $w == "load" ]; then
					echo "-------------------- Load --------------------"
				else
					echo "-------------------- Run ($w) --------------------"
				fi

				for n in 3 4 8 9; do
					ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/client/workloads-tachyon.sh $c $t $w $(printf '\r')\"" &
				done

				pending=0
				for n in 3 4 8 9; do
					read -p "Pending: ${pending} / 4"
					pending=$(expr $pending + 1)
				done
			done

			ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"${HOME}/hwchan/tachyon/bin/tachyon-stop.sh$(printf '\r')\""
			sleep 30

			for n in 3 4 8 9; do
				mkdir -p ${BASE_PATH}/results/workloads/tachyon/$iter/node$n
				scp testbed-node$n:${BASE_PATH}/results/workloads/$c/$t/*.txt ${BASE_PATH}/results/workloads/tachyon/$iter/node$n
				ssh testbed-node$n 'rm -rf ${BASE_PATH}/results/*'
			done

			echo "Finished experiment with coding scheme = $c and thread count = $t..."
		done
	done
done
