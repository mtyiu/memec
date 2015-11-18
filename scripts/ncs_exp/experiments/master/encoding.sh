#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio
CONTROL_NODE=testbed-node10

coding='raid0 raid1 raid5 rdp cauchy rs evenodd'
threads='64'

read -p "Press any key to start..."

for c in $coding; do
	echo "Preparing for the experiments with coding scheme = $c..."

	mkdir -p ${BASE_PATH}/results/encoding/$c

	for t in $threads; do
		echo "Running experiment with coding scheme = $c and thread count = $t..."
		${BASE_PATH}/scripts/ycsb/plio/load.sh $t 2>&1 | tee ${BASE_PATH}/results/encoding/$c/$t.txt

		echo "Finished experiment with coding scheme = $c and thread count = $t..."

		# Tell the control node that this iteration is finished
		ssh testbed-node10 "screen -S control -p 0 -X stuff \"$(printf '\r')\""

		read -p "Press any key to continue..."
	done
done

sed -i "s/^scheme=.*$/scheme=raid0/g" ${PLIO_PATH}/bin/config/ncs_exp/global.ini

${BASE_PATH}/scripts/util/rsync.sh
