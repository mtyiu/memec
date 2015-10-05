#!/bin/bash

# Usage: [n] [k] [chunk size] [raid5|cauchy|rdp|rs|evenodd] [k data files] [(n-k) parity files]

if [ $# != 5 ]; then
	echo "Usage: $0 [n] [k] [chunk size] [raid1|raid5|cauchy|rdp|rs|evenodd] [input path]"
	exit 1
fi

processed=0
succeeded=0
for i in $(ls $5/*.0.chunk); do
	prefix=$(sed 's/0\.chunk//g' <<< $i)
	count=$(ls $prefix* | wc -l)
	if [ $count == $1 ]; then
		processed=$(expr $processed + 1)
		if [ $4 == 'raid1' ]; then
			diff $prefix* 1> /dev/null 2>&1
		else
			./checker $1 $2 $3 $4 $prefix* 1> /dev/null 2>&1
		fi
		if [ $? != 0 ]; then
			# echo $prefix
			# exit 1
			echo -n
		else
			# echo $prefix
			succeeded=$(expr $succeeded + 1)
		fi
	fi
done

echo "Passed / Processed = $succeeded / $processed"
