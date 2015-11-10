#!/bin/bash

N=6
K=4
M=16

# Stripe List Generation Algorithm
./analysis $N $K $M | tee results.txt
echo "Finished generation."
gnuplot fairness.gpi

# Cleanup
rm results.txt
