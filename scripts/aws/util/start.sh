#!/bin/bash

BASE_PATH=${HOME}/mtyiu
BOOTSTRAP_SCRIPT_PATH=${BASE_PATH}/scripts/bootstrap

SLEEP_TIME=2

if [ $# -gt 0 ]; then
	echo "*** Warning: Debug mode is enabled. ***"
fi

# ./set_config.sh

screen -S coordinator -p 0 -X stuff "$(printf '\r\r')${BOOTSTRAP_SCRIPT_PATH}/start-plio-coordinator.sh ${1}$(printf '\r\r')"

sleep ${SLEEP_TIME}

for i in {1..10}; do
	ssh node$i "screen -S slave -p 0 -X stuff \"$(printf '\r\r')${BOOTSTRAP_SCRIPT_PATH}/start-plio-slave.sh ${1}$(printf '\r\r')\""
done

sleep ${SLEEP_TIME}

for i in {1..4}; do
	ssh client "screen -S master${i} -p 0 -X stuff \"$(printf '\r\r')${BOOTSTRAP_SCRIPT_PATH}/start-plio-master.sh ${1}$(printf '\r\r')\""
done

sleep ${SLEEP_TIME}

echo 1 > RUNNING
read -p "Press Enter to terminate all instances..."

killall -9 application coordinator master slave ycsb 1>&2 2> /dev/null
ssh client 'killall -9 application coordinator master slave ycsb 1>&2 2> /dev/null' &
for i in {1..10}; do
	ssh node$i 'killall -9 application coordinator master slave ycsb 1>&2 2> /dev/null' &
done

sleep ${SLEEP_TIME}

if [ $# -gt 0 ]; then
	# Debug mode
	TERM_COMMAND="$(printf '\r\r')quit$(printf '\r')clear$(printf '\r')"
else
	TERM_COMMAND="$(printf '\r\r')clear$(printf '\r')"
fi

for i in {1..10}; do
	ssh node$i "screen -S slave -p 0 -X stuff \"${TERM_COMMAND}\"" &
done
for i in {1..4}; do
	ssh client "screen -S master${i} -p 0 -X stuff \"${TERM_COMMAND}\"" &
	ssh client "screen -S ycsb${i} -p 0 -X stuff \"${TERM_COMMAND}\"" &
done
screen -S coordinator -p 0 -X stuff "${TERM_COMMAND}" &

sleep ${SLEEP_TIME}

echo 0 > RUNNING

clear
