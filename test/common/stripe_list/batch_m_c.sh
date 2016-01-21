#!/bin/bash

N=10
K=8
EPSILON=0.01

for N in 6 8 10 12 14; do
	K=$(($N-2))
	rm ${N}_${K}.tsv
	touch ${N}_${K}.tsv
	for M in $(seq $N 40); do
		./analysis_m_c $N $K $M $EPSILON >> ${N}_${K}.tsv
	done
done
