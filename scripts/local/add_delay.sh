#!/bin/bash

if [ $# != 3 ]; then
	echo "Usage: $0 [delay (in usec)] [node ID] [sleep time]"
	exit 1
fi

delay=$1
node=$2
sleep_time=$3

screen -S server$node -p 0 -X stuff \
		"delay$(printf '\r')100000$(printf '\r')"

sleep $sleep_time

screen -S server$node -p 0 -X stuff \
		"delay$(printf '\r')0$(printf '\r')"
