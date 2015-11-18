#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio

sed -i "s/^scheme=.*$/scheme=evenodd/g" ${PLIO_PATH}/bin/config/ncs_exp/global.ini
${BASE_PATH}/scripts/util/rsync.sh

for w in {1..8}; do
	echo "Preparing for the experiments for Workload #$w..."

	mkdir -p ${BASE_PATH}/results/memory/workload$w

	screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts/util/start.sh $1$(printf '\r')"
	sleep 30

	ssh testbed-node3 "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/master/memory.sh $w $t$(printf '\r')\""

	pending=0
	read -p "Waiting for completion..."

	echo "Done"

	screen -S manage -p 0 -X stuff "$(printf '\r\r')"
	sleep 10

	for n in {11..23} {37..39}; do
		scp testbed-node$n:${PLIO_PATH}/memory.log ${BASE_PATH}/results/memory/workload$w/$n.log
	done
done
