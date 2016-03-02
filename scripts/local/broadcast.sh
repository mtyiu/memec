#!/bin/bash

if [ $# == 0 ]; then
	echo "Usage: $0 [command]"
fi

for s in manage application client server1 server2 server3 server4 server5 server6 server7 server8 coordinator; do
	screen -S ${s} -p 0 -X stuff "$1 $(printf '\r')"
done
