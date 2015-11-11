#!/bin/bash

N=6
K=4
M=16

# Stripe List Generation Algorithm
rm results.txt
./analysis $N $K $M | tee results.txt
gnuplot fairness.gpi
