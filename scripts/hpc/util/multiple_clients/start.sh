#!/bin/bash

BASE_PATH=${HOME}/mtyiu
BOOTSTRAP_SCRIPT_PATH=${BASE_PATH}/scripts/bootstrap

SLEEP_TIME=2

if [ $# -gt 0 ]; then
	echo "*** Warning: Debug mode is enabled. ***"
fi

ssh hpc15 "screen -S coordinator -p 0 -X stuff \"$(printf '\r\r')${BOOTSTRAP_SCRIPT_PATH}/start-plio-coordinator.sh ${1}$(printf '\r\r')\""

sleep ${SLEEP_TIME}

for i in {1..6}; do
	port=$(expr $i + 9110)
	node_id=$(expr $i + 8)
	ssh hpc${node_id} "screen -S slave$i -p 0 -X stuff \"$(printf '\r\r')${BOOTSTRAP_SCRIPT_PATH}/start-plio-slave.sh ${1}$(printf '\r\r')\"" &
done

sleep ${SLEEP_TIME}

for i in {1..6}; do
	port=$(expr $i + 9110)
	node_id=$(expr $i + 8)
	ssh hpc${node_id} "screen -S master -p 0 -X stuff \"$(printf '\r\r')${BOOTSTRAP_SCRIPT_PATH}/start-plio-master.sh ${1}$(printf '\r\r')\""
done

sleep ${SLEEP_TIME}

read -p "Press Enter to terminate all instances..."

for i in {9..15}; do
	ssh hpc$i 'killall -9 application coordinator master slave ycsb >&/dev/null' &
done

sleep ${SLEEP_TIME}

if [ $# -gt 0 ]; then
	# Debug mode
	TERM_COMMAND="$(printf '\r\r')quit$(printf '\r')clear$(printf '\r')"
else
	TERM_COMMAND="$(printf '\r\r')clear$(printf '\r')"
fi

for i in {1..6}; do
	node_id=$(expr $i + 8)
	ssh hpc${node_id} "screen -S slave$i -p 0 -X stuff \"${TERM_COMMAND}\"" &
	ssh hpc${node_id} "screen -S master -p 0 -X stuff \"${TERM_COMMAND}\"" &
done
ssh hpc15 "screen -S ycsb -p 0 -X stuff \"${TERM_COMMAND}\"" &
ssh hpc15 "screen -S coordinator -p 0 -X stuff \"${TERM_COMMAND}\"" &

sleep ${SLEEP_TIME}

clear
