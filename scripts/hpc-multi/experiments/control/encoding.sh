#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec

coding='raid5 rdp cauchy rs evenodd'
threads='64'
workloads='load workloada workloadc'

for c in $coding; do
	echo "Preparing for the experiments with coding scheme = $c..."

	sed -i "s/^scheme=.*$/scheme=$c/g" ${MEMEC_PATH}/bin/config/hpc/global.ini

	for iter in {1..10}; do
		for t in $threads; do
			TIMESTAMP=$(date +%Y%m%d-%H%M%S)

			echo "Running experiment with coding scheme = $c and thread count = $t..."
			screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts-multi/util/start.sh $1$(printf '\r')"
			sleep 150

			for w in $workloads; do
				if [ $w == "load" ]; then
					echo "-------------------- Load --------------------"
				else
					echo "-------------------- Run ($w) --------------------"
				fi

				for n in {7..14}; do
					ssh hpc$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts-multi/experiments/client/encoding.sh $c $t $w $(printf '\r')\"" &
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

			echo "Finished experiment with coding scheme = $c and thread count = $t..."

			screen -S manage -p 0 -X stuff "$(printf '\r\r')"
			sleep 10

			for n in {7..14}; do
				mkdir -p ${BASE_PATH}/results-multi/encoding/$TIMESTAMP/hpc$n
				cp ${BASE_PATH}/results-multi/encoding/tmp/hpc$n/$c/$t/*.txt ${BASE_PATH}/results-multi/encoding/$TIMESTAMP/hpc$n
				rm -rf ${BASE_PATH}/results-multi/encoding/tmp/hpc$n/*
			done
		done
	done
done

sed -i "s/^scheme=.*$/scheme=raid0/g" ${MEMEC_PATH}/bin/config/hpc/global.ini
