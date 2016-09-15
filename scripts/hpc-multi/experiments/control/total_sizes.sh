#!/bin/bash

BASE_PATH=${HOME}/mtyiu
BOOTSTRAP_SCRIPT_PATH=${BASE_PATH}/scripts-multi/bootstrap
MEMEC_PATH=${BASE_PATH}/memec

sizes='1 2 4 6 8 10 12 14 16'
sizes='16'
# sizes=1

mkdir -p ${BASE_PATH}/results-multi/total_sizes/tmp/hpc15
for s in $sizes; do
	echo "Preparing for the experiments with total size = $s..."

	for iter in {1..1}; do
		TIMESTAMP=$(date +%Y%m%d-%H%M%S)

		screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts-multi/util/start.sh $1$(printf '\r')"
		sleep 40

		echo "-------------------- Load --------------------"

		for n in {7..14}; do
			ssh hpc$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts-multi/experiments/client/total_sizes.sh $s $(printf '\r')\"" &
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
	
		echo "Starting server at hpc15..."
		ssh hpc15 "screen -S server -p 0 -X stuff \"$(printf '\r\r')${BOOTSTRAP_SCRIPT_PATH}/start-server.sh ${1}$(printf '\r\r')\"" &
		ssh hpc15 "screen -S coordinator -p 0 -X stuff \"seal$(printf '\r')\""
		sleep 30

		ssh hpc15 "screen -S coordinator -p 0 -X stuff \"add$(printf '\r')hpc15$(printf '\r')tcp://137.189.88.46:9111$(printf '\r')\""
		sleep 5
		ssh hpc15 "screen -S coordinator -p 0 -X stuff \"migrate$(printf '\r')\""
		sleep 120

		echo "Finished experiment with total size = $s..."

		screen -S manage -p 0 -X stuff "$(printf '\r\r')"
		sleep 10

		for n in {7..15}; do
			mkdir -p ${BASE_PATH}/results-multi/total_sizes/memec/$s/$TIMESTAMP/hpc$n
			if [ "$n" == "15" ]; then
				cp ${BASE_PATH}/results-multi/total_sizes/tmp/hpc$n/*.txt ${BASE_PATH}/results-multi/total_sizes/memec/$s/$TIMESTAMP/hpc$n
			else
				cp ${BASE_PATH}/results-multi/total_sizes/tmp/hpc$n/$s/*.txt ${BASE_PATH}/results-multi/total_sizes/memec/$s/$TIMESTAMP/hpc$n
			fi
			rm -rf ${BASE_PATH}/results-multi/total_sizes/tmp/hpc$n/*
		done
	done
done
