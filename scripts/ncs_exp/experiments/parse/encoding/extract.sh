#!/bin/bash

if [ $# != 1 ]; then
	echo "Usage: $0 [encoding results dir]"
	exit 1
fi

coding='raid0 raid1 raid5 rs rdp evenodd cauchy'

output=""

for c in $coding; do
	output="$output $(head -n $(grep -n "\[INSERT\], Return=" evenodd/64.txt | sed 's/:.*$//g') $1/$c/*.txt | tail -n 5 | sed 's/^.*, //g')"
done

echo $output | python convert.py
