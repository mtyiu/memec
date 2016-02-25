#!/bin/bash

SLEEP_TIME=0.4

screen -S coordinator -p 0 -X stuff "bin/coordinator -v $(printf '\r')"
sleep ${SLEEP_TIME}

for i in {1..4}; do
	rm -rf /tmp/memec/server${i}
	mkdir -p /tmp/memec/server${i} 2> /dev/null
	screen -S server${i} -p 0 -X stuff \
		"bin/server -v -o server server${i} tcp://127.0.0.1:911${i} -o storage path /tmp/memec/server${i} $(printf '\r')"
	sleep ${SLEEP_TIME}
done
read -p "Press Enter to terminate all instances..."

killall coordinator server
