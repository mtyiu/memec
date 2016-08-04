#!/bin/bash

SLEEP_TIME=1

for i in {7..15}; do
	ssh hpc$i 'killall -9 application coordinator client server ycsb >& /dev/null' &
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
	ssh hpc${node_id} "screen -S client -p 0 -X stuff \"${TERM_COMMAND}\"" &
	ssh hpc${node_id} "screen -S ycsb -p 0 -X stuff \"${TERM_COMMAND}\"" &
done
ssh hpc15 "screen -S coordinator -p 0 -X stuff \"${TERM_COMMAND}\"" &

sleep ${SLEEP_TIME}

clear
