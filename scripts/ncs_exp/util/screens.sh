#!/bin/bash

BASE_PATH=${HOME}/mtyiu
BOOTSTRAP_SCRIPT_PATH=${BASE_PATH}/scripts/bootstrap

############
### START
############

function start_screens {

	## COORDINATOR ##
	ssh testbed-node1 "if [ -z \"\`screen -ls | grep coordinator | grep -v grep\`\" ]; then echo \"[Node 1] Creating screen for coordinator\"; screen -dmS coordinator; else echo \"[Node 1] Coordinator screen exists\"; fi"

	## SLAVE & ETHTOOL ##
	for i in {11..23} {37..39}; do
		ssh testbed-node$i "if [ -z \"\`screen -ls | grep slave | grep -v grep\`\" ]; then echo \"[Node $i] Creating screen for slave\"; screen -dmS slave; else echo \"[Node $i] Slave screen exists\"; fi"
		ssh testbed-node$i "if [ -z \"\`screen -ls | grep ethtool | grep -v grep\`\" ]; then echo \"[Node $i] Creating screen for ethtool\"; screen -dmS ethtool; else echo \"[Node $i] Ethtool screen exists\"; fi"
	done

	## MASTER & YCSB ##
	for i in 3 4 8 9; do
		ssh testbed-node$i "if [ -z \"\`screen -ls | grep master | grep -v grep\`\" ]; then echo \"[Node $i] Creating screen for master\"; screen -dmS master; else echo \"[Node $i] Master screen exists\"; fi"
		ssh testbed-node$i "if [ -z \"\`screen -ls | grep ycsb | grep -v grep\`\" ]; then echo \"[Node $i] Creating screen for ycsb\"; screen -dmS ycsb; else echo \"[Node $i] YCSB screen exists\"; fi"
	done

	## EXPERIMENT & MANAGE ##
	ssh testbed-node10 "if [ -z \"\`screen -ls | grep experiment | grep -v grep\`\" ]; then echo \"[Node 10] Creating screen for experiment\"; screen -dmS experiment; else echo \"[Node 10] Experiment screen exists\"; fi"
	ssh testbed-node10 "if [ -z \"\`screen -ls | grep manage | grep -v grep\`\" ]; then echo \"[Node 10] Creating screen for experiment\"; screen -dmS experiment; else echo \"[Node 10] Manage screen exists\"; fi"
}

############
### STOP
############

function stop_screens {

	## COORDINATOR ##
	ssh testbed-node1 "tmpid=\`screen -ls | grep coordinator | awk '{print \$1}' | awk -F '.' '{print \$1}'\`; if [ -z \"\$tmpid\" ]; then echo \"[Node 1] No screen for coordinator\"; else echo \"[Node 1] Kill coordinator screen\"; kill \$tmpid; fi"

	## SLAVE & ETHTOOL ##
	for i in {11..23} {37..39}; do
		ssh testbed-node${i} "tmpid=\`screen -ls | grep slave | awk '{print \$1}' | awk -F '.' '{print \$1}'\`; if [ -z \"\$tmpid\" ]; then echo \"[Node ${i}] No screen for slave\"; else echo \"[Node ${i}] Kill slave screen\"; kill \$tmpid; fi"
		ssh testbed-node${i} "tmpid=\`screen -ls | grep ethtool | awk '{print \$1}' | awk -F '.' '{print \$1}'\`; if [ -z \"\$tmpid\" ]; then echo \"[Node ${i}] No screen for ethtool\"; else echo \"[Node ${i}] Kill ethtool screen\"; kill \$tmpid; fi"
	done

	## MASTER & YCSB ##
	for i in 3 4 8 9; do
		ssh testbed-node${i} "tmpid=\`screen -ls | grep master | awk '{print \$1}' | awk -F '.' '{print \$1}'\`; if [ -z \"\$tmpid\" ]; then echo \"[Node ${i}] No screen for master\"; else echo \"[Node ${i}] Kill master screen\"; kill \$tmpid; fi"
		ssh testbed-node${i} "tmpid=\`screen -ls | grep ycsb | awk '{print \$1}' | awk -F '.' '{print \$1}'\`; if [ -z \"\$tmpid\" ]; then echo \"[Node ${i}] No screen for ycsb\"; else echo \"[Node ${i}] Kill ycsb screen\"; kill \$tmpid; fi"
	done

}

if [ $# -ge 1 ] && [ "$1" == "stop" ]; then
	stop_screens	
else
	start_screens
fi
