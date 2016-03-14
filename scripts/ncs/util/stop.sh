#!/bin/bash

SLEEP_TIME=1

for i in {1..23} {37..39}; do
	ssh testbed-node$i 'killall -9 application coordinator client server ycsb 1>&2 2> /dev/null' &
done

sleep ${SLEEP_TIME}

if [ $# -gt 0 ]; then
	# Debug mode
	TERM_COMMAND="$(printf '\r\r')quit$(printf '\r')clear$(printf '\r')"
else
	TERM_COMMAND="$(printf '\r\r')clear$(printf '\r')"
fi

for i in {11..23} {37..39}; do
	ssh testbed-node$i "screen -S server -p 0 -X stuff \"${TERM_COMMAND}\"" &
done
for i in 3 4 8 9; do
	ssh testbed-node$i "screen -S client -p 0 -X stuff \"${TERM_COMMAND}\"" &
	ssh testbed-node$i "screen -S ycsb -p 0 -X stuff \"${TERM_COMMAND}\"" &
done
ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"${TERM_COMMAND}\"" &

sleep ${SLEEP_TIME}

echo 0 > RUNNING

clear
