#!/bin/bash

SLEEP_TIME=0.4

screen -S coordinator -p 0 -X stuff "bin/coordinator -v $(printf '\r')"
sleep ${SLEEP_TIME}

for i in {1..8}; do
	rm -rf /tmp/plio/slave${i}
	mkdir -p /tmp/plio/slave${i} 2> /dev/null
	screen -S slave${i} -p 0 -X stuff \
		"bin/slave -v -o slave slave${i} tcp://127.0.0.1:911${i} -o storage path /tmp/plio/slave${i} $(printf '\r')"
done

sleep 3

screen -S master -p 0 -X stuff "bin/master -v $(printf '\r')"
sleep ${SLEEP_TIME}

screen -S application -p 0 -X stuff "bin/application -v $(printf '\r')"
sleep ${SLEEP_TIME}

screen -S application -p 0 -X stuff "set 4084 ../4084 $(printf '\r')"
sleep ${SLEEP_TIME}
screen -S application -p 0 -X stuff "set 2 ../4084 $(printf '\r')"
sleep ${SLEEP_TIME}
screen -S application -p 0 -X stuff "set 33 ../4084 $(printf '\r')"
sleep ${SLEEP_TIME}

read -p "Press Enter to terminate all instances..."

killall application coordinator master slave
# screen -S slave1 -p 0 -X stuff "quit$(printf '\r')y$(printf '\r')"

# for s in application master slave1 slave2 slave3 slave4 coordinator; do
# 	screen -S ${s} -p 0 -X stuff "exit $(printf '\r')"
# done
