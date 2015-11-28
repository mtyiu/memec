#!/bin/bash

for iter in {1..30}; do
	./exp4_control1.sh $iter
	./exp4_control2.sh $iter
	./degraded_control.sh $iter
done
