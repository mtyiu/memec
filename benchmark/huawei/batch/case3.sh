#!/bin/bash

BASE_PATH=${HOME}/mtyiu

cd ${BASE_PATH}/plio/benchmark/huawei

DATA_SIZE=10240

for TOTAL_SIZE in 1 3 5 10; do
	ssh hpc15 "screen -S manage -p 0 -X stuff \"$(printf '\r\r')${BASE_PATH}/scripts/util/start.sh$(printf '\r\r')\""
	sleep 20

	scripts/case3.sh ${DATA_SIZE} ${TOTAL_SIZE} 1 2>&1 | tee output/exp3_${TOTAL_SIZE}.out

	ssh hpc15 "screen -S manage -p 0 -X stuff \"$(printf '\r\r')\""
done
