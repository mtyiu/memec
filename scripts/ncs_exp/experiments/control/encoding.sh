#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio

coding='raid0 raid1 raid5 rdp cauchy rs evenodd'
threads='64'

for c in $coding; do
	echo "Preparing for the experiments with coding scheme = $c..."

	mkdir -p ${BASE_PATH}/results/encoding/$c

	for t in $threads; do
		echo "Running experiment with coding scheme = $c and thread count = $t..."
		screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts/util/start.sh $1$(printf '\r')"
		sleep 30

		for n in 3 4 8 9; do
			ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/master/encoding.sh $c $t$(printf '\r')\"" &
		done

		pending=0
		for n in 3 4 8 9; do
			read -p "Pending: ${pending} / 4"
			pending=$(expr $pending + 1)
		done

		echo "Finished experiment with coding scheme = $c and thread count = $t..."

		screen -S manage -p 0 -X stuff "$(printf '\r\r')"
		sleep 10

		for n in 3 4 8 9; do
			scp -r testbed-node$n:${BASE_PATH}/results/encoding/$c ${BASE_PATH}/results/encoding/$c/node$n
		done
	done
done

sed -i "s/^scheme=.*$/scheme=raid0/g" ${PLIO_PATH}/bin/config/ncs_exp/global.ini

${BASE_PATH}/scripts/util/rsync.sh
