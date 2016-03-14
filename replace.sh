#!/bin/bash

if [ "$1" == '' ] || [ "$2" == '' ]; then
	echo "Usage: $0 [Target] [Replace]"
	exit 1
fi

find . -name '*.cc' -exec sed -i "s/$1/$2/g" {} \;
find . -name '*.hh' -exec sed -i "s/$1/$2/g" {} \;
find . -name '*.ini' -exec sed -i "s/$1/$2/g" {} \;
find . -name '*.sh' -exec sed -i "s/$1/$2/g" {} \;
find . -name '*.md' -exec sed -i "s/$1/$2/g" {} \;
