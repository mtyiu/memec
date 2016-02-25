#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec

# coding='raid0 raid1 raid5 rdp cauchy rs evenodd'
# threads='16 32 64 128 256 512 1000'
coding='evenodd'
threads='32'

for c in $coding; do
	echo "Preparing for the experiments with coding scheme = $c..."

	sed -i "s/^scheme=.*$/scheme=$c/g" ${MEMEC_PATH}/bin/config/hpc/global.ini

	for t in $threads; do
		mkdir -p ${BASE_PATH}/results/multiple_clients/$c/$t

		echo "Running experiment with coding scheme = $c and thread count = $t..."
		screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts/util/multiple_clients/start.sh $1$(printf '\r')"
		sleep 10
		for i in {1..6}; do
			node_id=$(expr $i + 8)
			ycsb_id=$(expr $i - 1)
			ssh hpc${node_id} "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/ycsb/memec/load.sh $t $ycsb_id 2>&1 | tee ${BASE_PATH}/results/multiple_clients/$c/$t/$i.txt\"" &
		done
		read -p "Press Enter at the ycsb screen in hpc9-14 to start the experiment..."
		read -p "Press Enter again to terminate all instances..."
		
		screen -S manage -p 0 -X stuff "$(printf '\r\r')"
		sleep 10
		echo "Finished experiment with coding scheme = $c and thread count = $t..."
	done
done
	
sed -i "s/^scheme=.*$/scheme=raid0/g" ${MEMEC_PATH}/bin/config/hpc/global.ini
