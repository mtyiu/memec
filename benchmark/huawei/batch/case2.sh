#!/bin/bash

BASE_PATH=${HOME}/mtyiu

cd ${BASE_PATH}/plio/benchmark/huawei

for NUM_CLIENTS in 1 3 5 10 20 30; do
	ssh hpc15 "screen -S manage -p 0 -X stuff \"$(printf '\r\r')${BASE_PATH}/scripts/util/start.sh$(printf '\r\r')\""
	sleep 20

	for DATA_SIZE in 512 1024 5120 10240; do
		scripts/case2.sh ${DATA_SIZE} ${NUM_CLIENTS} 2>&1 | tee output/exp2_${NUM_CLIENTS}_${DATA_SIZE}.out
	done

	ssh hpc15 "screen -S manage -p 0 -X stuff \"$(printf '\r\r')\""
done
