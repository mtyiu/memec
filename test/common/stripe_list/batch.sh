#!/bin/bash

N=6
K=4
M=16
C_FROM=20
C_TO=21
ITERS=10
LOAD_F=results/load.txt
RAND25_F=results/rand25.txt
RAND50_F=results/rand50.txt
RAND75_F=results/rand75.txt

# Stripe List Generation Algorithm
rm -rf results
mkdir results
echo ./analysis $N $K $M $C_FROM $C_TO $ITERS $LOAD_F $RAND25_F $RAND50_F $RAND75_F
./analysis $N $K $M $C_FROM $C_TO $ITERS $LOAD_F $RAND25_F $RAND50_F $RAND75_F
# gnuplot fairness.gpi
