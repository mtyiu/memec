#!/bin/bash

N=10
K=8
EPSILON=0.01

for M in {10..40}; do
	./analysis_m_c $N $K $M $EPSILON
done
