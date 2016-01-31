#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio

sizes='1000000000 2000000000 4000000000 8000000000 10000000000'

for s in $sizes; do
	for iter in {1..10}; do
		mkdir -p ${BASE_PATH}/results/recovery-double/$s/$iter

		echo "Preparing for the experiments for size = $s..."

		screen -S manage -p 0 -X stuff "${BASE_PATH}/scripts/util/start.sh $1$(printf '\r')"
		for i in 2 5 6 7; do
			ssh testbed-node$i "screen -S slave -p 0 -X stuff \"$(printf '\r\r')${BASE_PATH}/scripts/bootstrap/start-plio-backup-slave.sh ${1}$(printf '\r\r')\""
		done
		read -p "Press Enter when ready..." -t 30
		# sleep 30

		echo "Writing $s bytes of data to the system..."
		size=$(expr $s \/ 4)
		for n in 3 4 8 9; do
			ssh testbed-node$n "screen -S ycsb -p 0 -X stuff \"${BASE_PATH}/scripts/experiments/master/recovery.sh $size $(printf '\r')\"" &
		done

		pending=0
		echo "Start waiting at: $(date)..."
		for n in 3 4 8 9; do
			if [ $n == 3 ]; then
				read -p "Pending: ${pending} / 4" -t $(expr $s \/ 25000000)
			else
				read -p "Pending: ${pending} / 4" -t 10
			fi
			pending=$(expr $pending + 1)
		done
		echo "Done at: $(date)."

		ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"$(printf '\r\r')seal$(printf '\r\r')\""
		sleep 4

		n1=$(expr $RANDOM % 13 + 11)
		n2=$(expr $RANDOM % 13 + 11)
		while [ $n1 == $n2 ]; do
			n2=$(expr $RANDOM % 13 + 11)
		done
		echo "Killing node $n1 & $n2..."

		ssh testbed-node${n1} "screen -S slave -p 0 -X stuff \"$(printf '\r\r')sync$(printf '\r\r')\""
		ssh testbed-node${n2} "screen -S slave -p 0 -X stuff \"$(printf '\r\r')sync$(printf '\r\r')\""
		sleep 5
		ssh testbed-node${n1} "screen -S slave -p 0 -X stuff \"$(printf '\r\r')memory$(printf '\r\r')\""
		ssh testbed-node${n2} "screen -S slave -p 0 -X stuff \"$(printf '\r\r')memory$(printf '\r\r')\""
		sleep 1
		ssh testbed-node${n1} "killall -9 slave; screen -S slave -p 0 -X stuff \"$(printf '\r\r')\"" &
		ssh testbed-node${n2} "killall -9 slave; screen -S slave -p 0 -X stuff \"$(printf '\r\r')\""
		sleep 2

		# sleep 20
		read -p "Press Enter after recovery (1)..." -t $(expr $s \/ 25000000)
		read -p "Press Enter after recovery (2)..." -t $(expr $s \/ 25000000)

		ssh testbed-node1 "screen -S coordinator -p 0 -X stuff \"$(printf '\r\r')log$(printf '\r\r')\""
		sleep 2

		screen -S manage -p 0 -X stuff "$(printf '\r\r')"

		if [ $# -gt 0 ]; then
			# Debug mode
			TERM_COMMAND="$(printf '\r\r')quit$(printf '\r')clear$(printf '\r')"
		else
			TERM_COMMAND="$(printf '\r\r')clear$(printf '\r')"
		fi
		for i in 2 5 6 7; do
			ssh testbed-node$i "screen -S slave -p 0 -X stuff \"${TERM_COMMAND}\"" &
		done

		sleep 10

		scp testbed-node$n1:${PLIO_PATH}/memory.log ${BASE_PATH}/results/recovery-double/$s/$iter/memory-${n1}.log
		scp testbed-node$n2:${PLIO_PATH}/memory.log ${BASE_PATH}/results/recovery-double/$s/$iter/memory-${n2}.log
		scp testbed-node1:${PLIO_PATH}/coordinator.log ${BASE_PATH}/results/recovery-double/$s/$iter/
	done
done
