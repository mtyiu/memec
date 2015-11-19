#!/bin/bash

node=(4 6 2)
bd=10
obd=1000

if [ $# -lt 1 ]; then
	echo "$0 [cpu|net]"
	exit -1
fi

if [ "$1" == "cpu" ]; then
	echo "Not implemented"
elif [ "$1" == "net" ]; then
	for (( i=0; $i < ${#node[@]}; i+=1 )); do
		if [ $# -lt 2 ]; then
				echo "Lowering the bandwidth of node ${node[$i]} to $bd Mbps"
				ssh node${node[$i]} "sudo ethtool -s eth0 speed $bd duplex full"
		else
			echo "Restoring the bandwidth of node ${node[$i]} to $obd Mbps"
			ssh node${node[$i]} "sudo ethtool -s eth0 speed $obd duplex full"
		fi
	done
fi
