#!/bin/bash

SLEEP_TIME=1

for i in {9..15}; do
	ssh hpc$i 'killall -9 application coordinator master slave ycsb >& /dev/null' &
done

sleep ${SLEEP_TIME}

if [ $# -gt 0 ]; then
	# Debug mode
	TERM_COMMAND="$(printf '\r\r')quit$(printf '\r')clear$(printf '\r')"
else
	TERM_COMMAND="$(printf '\r\r')clear$(printf '\r')"
fi

for i in {1..7}; do
	node_index=$(expr $i + 8)
	ssh hpc${node_index} "screen -S slave$i -p 0 -X stuff \"${TERM_COMMAND}\"" &
done
ssh hpc15 "screen -S master -p 0 -X stuff \"${TERM_COMMAND}\"" &
ssh hpc15 "screen -S ycsb -p 0 -X stuff \"${TERM_COMMAND}\"" &
ssh hpc15 "screen -S coordinator -p 0 -X stuff \"${TERM_COMMAND}\"" &

sleep ${SLEEP_TIME}

clear
