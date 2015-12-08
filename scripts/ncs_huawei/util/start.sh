#!/bin/bash

BASE_PATH=${HOME}/mtyiu
BOOTSTRAP_SCRIPT_PATH=${BASE_PATH}/scripts_huawei/bootstrap

SLEEP_TIME=2

if [ $# -gt 0 ]; then
	echo "*** Warning: Debug mode is enabled. ***"
fi

# ./set_config.sh

ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"$(printf '\r\r')${BOOTSTRAP_SCRIPT_PATH}/start-plio-coordinator.sh ${1}$(printf '\r\r')\""

sleep ${SLEEP_TIME}

for i in {19..23} {37..39}; do
	ssh testbed-node$i "screen -S slave -p 0 -X stuff \"$(printf '\r\r')${BOOTSTRAP_SCRIPT_PATH}/start-plio-slave.sh ${1}$(printf '\r\r')\""
done

sleep ${SLEEP_TIME}

for i in {11..18}; do
	ssh testbed-node$i "screen -S master -p 0 -X stuff \"$(printf '\r\r')${BOOTSTRAP_SCRIPT_PATH}/start-plio-master.sh ${1}$(printf '\r\r')\""
done

sleep ${SLEEP_TIME}

echo 1 > RUNNING
read -p "Press Enter to terminate all instances..."

for i in {1..23} {37..39}; do
	ssh testbed-node$i 'killall -9 application coordinator master slave ycsb 1>&2 2> /dev/null' &
done

sleep ${SLEEP_TIME}

if [ $# -gt 0 ]; then
	# Debug mode
	TERM_COMMAND="$(printf '\r\r')quit$(printf '\r')clear$(printf '\r')"
else
	TERM_COMMAND="$(printf '\r\r')clear$(printf '\r')"
fi

for i in {19..23} {37..39}; do
	ssh testbed-node$i "screen -S slave -p 0 -X stuff \"${TERM_COMMAND}\"" &
done
for i in {11..18}; do
	ssh testbed-node$i "screen -S master -p 0 -X stuff \"${TERM_COMMAND}\"" &
	ssh testbed-node$i "screen -S ycsb -p 0 -X stuff \"${TERM_COMMAND}\"" &
done
ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"${TERM_COMMAND}\"" &

sleep ${SLEEP_TIME}

echo 0 > RUNNING

clear