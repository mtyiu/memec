#!/bin/bash

if [ $# == 0 ]; then
	echo "Usage: $0 [command]"
fi

for s in application master slave1 slave2 slave3 slave4 coordinator; do
	screen -S ${s} -p 0 -X stuff "$1 $(printf '\r')"
done
