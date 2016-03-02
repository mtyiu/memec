#!/bin/bash

################################################
#
# Script for parsing YCSB results
#
# INPUT:     folder containing the results in ".txt" files
# OUTPUT:    A line containing the following fields for each file, to a designated file
#            1. File name (without extension)
#            2. Runtime
#            3. Length of fields
#            4. Number of records
#            5. Throughput ( ( ( length of fields + legnth of key ) * # of record ) / # of operations )
#            6. Average Latency for INSERT
#            7. Minimum Latency for INSERT
#            8. Maximum Latency for INSERT
#
################################################

if [ $# != 1 ]; then
	echo "Usage: $0 [path to the log folder]"
	exit 1
elif [ ! -d $1 ]; then
	echo "Error: The log folder does not exist."
	exit 1
fi

prefix_length=$(expr ${#1} + 2)
list=$(ls $1/[^r]*.txt | sort -k1.${prefix_length} -n)
output=$1/results.txt
header=~/mtyiu/scripts/ycsb/header.txt

header=$(cat ${header})

echo $header > ${output}

for f in $list; do
	thread=$(sed "s/\.txt//g" <<< $f | sed "s/^.*\///g")
	echo -n -e "$thread\t" >> ${output}

	elapsed_time=$(grep "\[OVERALL\], RunTime(ms), " $f | sed "s/\[OVERALL\], RunTime(ms), //g")
	elapsed_time=$(echo "scale=3; ${elapsed_time} / 1000.0" | bc -l)
	echo -n $elapsed_time >> ${output}
	echo -n -e "\t" >> ${output}

	data_size=$(grep "fieldlength" $f | head -n1 | sed 's/^.*fieldlength=\([0-9]*\) .*$/\1/g')
	data_size=$(expr ${data_size} + 40)

	num_ops=$(grep "recordcount" $f | head -n1 | sed 's/^.*recordcount=\([0-9]*\) .*$/\1/g')

	total_data_size=$(expr ${data_size} \* ${num_ops})
	throughput=$(echo ${total_data_size} / 1024 / 1024 / ${elapsed_time} | bc -l)

	echo -n -e "${throughput}\t" >> ${output}

	grep "\[INSERT\], AverageLatency(us), " $f | sed "s/\[INSERT\], AverageLatency(us), //g" | xargs echo -n >> ${output}
	echo -n -e "\t" >> ${output}

	grep "\[INSERT\], MinLatency(us), " $f | sed "s/\[INSERT\], MinLatency(us), //g" | xargs echo -n >> ${output}
	echo -n -e "\t" >> ${output}

	grep "\[INSERT\], MaxLatency(us), " $f | sed "s/\[INSERT\], MaxLatency(us), //g" | xargs echo -n >> ${output}
	echo >> ${output}
done

cat ${output}
