#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec
REDIS_PATH=${BASE_PATH}/redis

if [ $# != 1 ]; then
	echo "Usage: $1 [memec/redis]"
fi

sizes='8 32 64 128 256 512 1024 2048 4096 8192 16384'
workloads='workloada workloadc'

for s in $sizes; do
	echo "Preparing for the experiments with value size = $s..."

	TARGET=${BASE_PATH}/results/data_sizes/$1/$s/$(date +%Y%m%d-%H%M%S)

	mkdir -p ${TARGET}

	if [ $1 == 'memec' ]; then
		screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts/util/start.sh $1$(printf '\r')"
		sleep 10
		${BASE_PATH}/scripts/ycsb/memec/load-size.sh $s 2>&1 | tee ${TARGET}/load.txt

		for w in $workloads; do
			${BASE_PATH}/scripts/ycsb/memec/run-size.sh $s $w 2>&1 | tee ${TARGET}/$w.txt
		done

		screen -S manage -p 0 -X stuff "$(printf '\r\r')"
		sleep 10
	else
		screen -S manage -p 0 -X stuff "${REDIS_PATH}/utils/create-cluster-distributed start$(printf '\r')"
		sleep 10
		screen -S manage -p 0 -X stuff "${REDIS_PATH}/utils/create-cluster-distributed create$(printf '\r')"
		sleep 5
		screen -S manage -p 0 -X stuff "yes$(printf '\r')"
		sleep 10

		${BASE_PATH}/scripts/ycsb/redis/load-size.sh $s 2>&1 | tee ${TARGET}/load.txt

		for w in $workloads; do
			${BASE_PATH}/scripts/ycsb/redis/run-size.sh $s $w 2>&1 | tee ${TARGET}/$w.txt
		done

		screen -S manage -p 0 -X stuff "${REDIS_PATH}/utils/create-cluster-distributed stop$(printf '\r')"
		sleep 2
		screen -S manage -p 0 -X stuff "${REDIS_PATH}/utils/create-cluster-distributed clean$(printf '\r')"
		sleep 5
	fi
done
