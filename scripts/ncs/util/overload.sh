#!/bin/bash

node=4
bd=100
obd=1000

if [ $# -lt 1 ]; then
	echo "$0 [cpu|net]"
	exit -1
fi

if [ "$1" == "cpu" ]; then
	echo "Not implemented"
elif [ "$1" == "net" ]; then
	if [ $# -lt 2 ]; then
		echo "Lowering the bandwidth of node $node to $bd Mbps"
		ssh node${node} "sudo ethtool -s eth0 speed $bd duplex full"
	else
		echo "Restoring the bandwidth of node $node to $obd Mbps"
		ssh node${node} "sudo ethtool -s eth0 speed $obd duplex full"
	fi
fi
