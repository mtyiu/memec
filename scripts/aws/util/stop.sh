#!/bin/bash

SLEEP_TIME=1

killall -9 application coordinator client server ycsb 1>&2 2> /dev/null
ssh client 'killall -9 application coordinator client server ycsb 1>&2 2> /dev/null' &
for i in {1..10}; do
	ssh node$i 'killall -9 application coordinator client server ycsb 1>&2 2> /dev/null' &
done

sleep ${SLEEP_TIME}

if [ $# -gt 0 ]; then
	# Debug mode
	TERM_COMMAND="$(printf '\r\r')quit$(printf '\r')clear$(printf '\r')"
else
	TERM_COMMAND="$(printf '\r\r')clear$(printf '\r')"
fi

for i in {1..10}; do
	ssh node$i "screen -S server -p 0 -X stuff \"${TERM_COMMAND}\"" &
done
for i in {1..4}; do
	ssh client "screen -S client${i} -p 0 -X stuff \"${TERM_COMMAND}\"" &
	ssh client "screen -S ycsb${i} -p 0 -X stuff \"${TERM_COMMAND}\"" &
done
screen -S coordinator -p 0 -X stuff "${TERM_COMMAND}" &

sleep ${SLEEP_TIME}

echo 0 > RUNNING

clear
