#!/bin/bash

i=0

while true; do
	input=$(strings /dev/urandom | grep -o '[[:alnum:]]' | head -n 30 | tr -d '\n'; echo)
	i=$((i+1))
	cpp=$(./hash $input)
	java=$(java Hash $input)
	diff  <(echo "$cpp" ) <(echo "$java")
	echo -e -n "\rCompared: $i"
done
