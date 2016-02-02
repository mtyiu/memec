#!/bin/bash

SLEEP_TIME=0.4
# VERBOSE=-v

screen -S coordinator -p 0 -X stuff "bin/coordinator ${VERBOSE} 2>&1 | tee coordinator.txt $(printf '\r')"
sleep ${SLEEP_TIME}

for i in {1..10}; do
	rm -rf /tmp/plio/slave${i}
	mkdir -p /tmp/plio/slave${i} 2> /dev/null
	port=$(expr $i + 9110)
	screen_id=slave${i}
	if [ $i == 10 ]; then
		screen_id=slave_10
	fi
	if [ $# == 0 ]; then
		screen -S ${screen_id} -p 0 -X stuff \
			"bin/slave ${VERBOSE} -o slave slave${i} tcp://127.0.0.1:${port} -o storage path /tmp/plio/slave${i} 2>&1 | tee ${port}.txt $(printf '\r')"
	else
		screen -S slave${i} -p 0 -X stuff \
			"gdb bin/slave -ex \"r -v -o slave slave${i} tcp://127.0.0.1:911${i} -o storage path /tmp/plio/slave${i}\" $(printf '\r')  $(printf '\r') $(printf '\r') $(printf '\r')"
	fi
done

# sleep 1

screen -S master -p 0 -X stuff "bin/master ${VERBOSE} 2>&1 | tee master.txt $(printf '\r')"
sleep ${SLEEP_TIME}

# screen -S application -p 0 -X stuff "bin/application -v $(printf '\r')"
# sleep ${SLEEP_TIME}

read -p "Press Enter to terminate all instances..."

killall application coordinator master slave

# for s in application master slave1 slave2 slave3 slave4 coordinator; do
# 	screen -S ${s} -p 0 -X stuff "exit $(printf '\r')"
# done
