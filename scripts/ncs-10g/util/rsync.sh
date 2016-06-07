#!/bin/bash

for i in {11..20} {30..34}; do
	rsync \
		--delete \
		--force \
		--progress \
		--verbose \
		--archive \
		~/mtyiu/memec/ testbed-node$i:mtyiu/memec/
done
