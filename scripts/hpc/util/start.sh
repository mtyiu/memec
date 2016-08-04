#!/bin/bash

BASE_PATH=${HOME}/mtyiu
BOOTSTRAP_SCRIPT_PATH=${BASE_PATH}/scripts/bootstrap

SLEEP_TIME=2

if [ $# -gt 0 ]; then
	echo "*** Warning: Debug mode is enabled. ***"
fi

ssh hpc15 "screen -S coordinator -p 0 -X stuff \"$(printf '\r\r')${BOOTSTRAP_SCRIPT_PATH}/start-coordinator.sh ${1}$(printf '\r\r')\""

sleep ${SLEEP_TIME}

for i in {7..14}; do
	node_id=$i
	ssh hpc${node_id} "screen -S server -p 0 -X stuff \"$(printf '\r\r')${BOOTSTRAP_SCRIPT_PATH}/start-server.sh ${1}$(printf '\r\r')\"" &
done

sleep ${SLEEP_TIME}
sleep ${SLEEP_TIME}

ssh hpc15 "screen -S client -p 0 -X stuff \"$(printf '\r\r')${BOOTSTRAP_SCRIPT_PATH}/start-client.sh ${1}$(printf '\r\r')\""
sleep 5

sleep ${SLEEP_TIME}

read -p "Press Enter to terminate all instances..."

for i in {7..15}; do
	ssh hpc$i 'killall -9 application coordinator client server ycsb >&/dev/null' &
done

sleep ${SLEEP_TIME}

if [ $# -gt 0 ]; then
	# Debug mode
	TERM_COMMAND="$(printf '\r\r')quit$(printf '\r')clear$(printf '\r')"
else
	TERM_COMMAND="$(printf '\r\r')clear$(printf '\r')"
fi

for i in {7..14}; do
	node_id=$i
	ssh hpc${node_id} "screen -S server -p 0 -X stuff \"${TERM_COMMAND}\"" &
done
ssh hpc15 "screen -S client -p 0 -X stuff \"${TERM_COMMAND}\"" &
ssh hpc15 "screen -S ycsb -p 0 -X stuff \"${TERM_COMMAND}\"" &
ssh hpc15 "screen -S coordinator -p 0 -X stuff \"${TERM_COMMAND}\"" &

sleep ${SLEEP_TIME}

clear
