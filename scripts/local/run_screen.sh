#!/bin/bash

SLEEP_TIME=0.4
# VERBOSE=-v

screen -S coordinator -p 0 -X stuff "bin/coordinator ${VERBOSE} 2>&1 | tee coordinator.txt $(printf '\r')"
sleep ${SLEEP_TIME}

for i in {1..10}; do
	rm -rf /tmp/plio/server${i}
	mkdir -p /tmp/plio/server${i} 2> /dev/null
	port=$(expr $i + 9110)
	screen_id=server${i}
	if [ $i == 10 ]; then
		screen_id=server_10
	fi
	if [ $# == 0 ]; then
		screen -S ${screen_id} -p 0 -X stuff \
			"bin/server ${VERBOSE} -o server server${i} tcp://127.0.0.1:${port} -o storage path /tmp/plio/server${i} 2>&1 | tee ${port}.txt $(printf '\r')"
	else
		screen -S server${i} -p 0 -X stuff \
			"gdb bin/server -ex \"r -v -o server server${i} tcp://127.0.0.1:911${i} -o storage path /tmp/plio/server${i}\" $(printf '\r')  $(printf '\r') $(printf '\r') $(printf '\r')"
	fi
done

# sleep 1

screen -S client -p 0 -X stuff "bin/client ${VERBOSE} 2>&1 | tee client.txt $(printf '\r')"
sleep ${SLEEP_TIME}

# screen -S application -p 0 -X stuff "bin/application -v $(printf '\r')"
# sleep ${SLEEP_TIME}

read -p "Press Enter to terminate all instances..."

killall application coordinator client server

# for s in application client server1 server2 server3 server4 coordinator; do
# 	screen -S ${s} -p 0 -X stuff "exit $(printf '\r')"
# done
