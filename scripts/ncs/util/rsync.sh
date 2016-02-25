#!/bin/bash

for i in {1..8} 10; do
	rsync \
		--delete \
		--force \
		--progress \
		--verbose \
		--archive \
		~/mtyiu/memec/ testbed-node$i:mtyiu/memec/
done
