#!/bin/bash

N=6
K=4
M=16
C_FROM=1
C_TO=100
ITERS=1000
LOAD_F=results/load.txt
RAND10_F=results/rand10.txt
RAND50_F=results/rand50.txt
RAND90_F=results/rand90.txt

# Stripe List Generation Algorithm
rm -rf fairness
rm -rf results
mkdir fairness results
cp fairness.html fairness/
./analysis $N $K $M $C_FROM $C_TO $ITERS $LOAD_F $RAND10_F $RAND50_F $RAND90_F

declare -a metrics=( 'load' 'cost' 'keys' )
declare -a fairnesses=( 'maxmin' 'stdev' 'jains' )
declare -a fairnesses_full=( 'Max-min Average' 'Standard Deviation' "Jain's Fairness" )

for i in {1..3}; do
	metric=${metrics[ $i - 1 ]}
	for j in {1..3}; do
		fairness=${fairnesses[ $j - 1 ]}
		fairness_full=${fairnesses_full[ $j - 1 ]}
		(( index = ( i - 1 ) * 3 + j + 1 ))
		echo $metric $fairness $fairness_full $index

		gnuplot -e "metric='${metric}'; fairness='${fairness}'; fairness_full=\"${fairness_full}\"; index='${index}'" fairness.gpi
	done
done
