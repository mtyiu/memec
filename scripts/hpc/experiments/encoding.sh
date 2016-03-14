#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec

coding='raid0 raid1 raid5 rdp cauchy rs evenodd'
threads='16 32 64 128 256 512 1000'

for c in $coding; do
	echo "Preparing for the experiments with coding scheme = $c..."

	sed -i "s/^scheme=.*$/scheme=$c/g" ${MEMEC_PATH}/bin/config/hpc/global.ini

	mkdir -p ${BASE_PATH}/results/encoding/$c

	for t in $threads; do
		echo "Running experiment with coding scheme = $c and thread count = $t..."
		screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts/util/start.sh $1$(printf '\r')"
		sleep 10
		${BASE_PATH}/scripts/ycsb/memec/load.sh $t 2>&1 | tee ${BASE_PATH}/results/encoding/$c/$t.txt
		screen -S manage -p 0 -X stuff "$(printf '\r\r')"
		sleep 10
		echo "Finished experiment with coding scheme = $c and thread count = $t..."
	done
done
	
sed -i "s/^scheme=.*$/scheme=raid0/g" ${MEMEC_PATH}/bin/config/hpc/global.ini
