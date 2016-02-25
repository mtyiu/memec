#!/bin/bash

BASE_PATH=${HOME}/mtyiu

cd ${BASE_PATH}/memec/benchmark/huawei

DATA_SIZE=10240

for NUM_CLIENTS in 2 4 6; do
	for TOTAL_SIZE in 1 3 5 10; do
		ssh hpc15 "screen -S manage -p 0 -X stuff \"$(printf '\r\r')${BASE_PATH}/scripts/util/start.sh$(printf '\r\r')\""
		sleep 20

		scripts/case4.sh ${DATA_SIZE} ${TOTAL_SIZE} ${NUM_CLIENTS} 2>&1 | tee output/exp4_${NUM_CLIENTS}_${TOTAL_SIZE}.out

		ssh hpc15 "screen -S manage -p 0 -X stuff \"$(printf '\r\r')\""
	done
done
