#!/bin/bash

for i in {1..10} {11..23} {37..39}; do
	rsync \
		--delete \
		--force \
		--progress \
		--verbose \
		--archive \
		~/mtyiu/memec/ testbed-node$i:mtyiu/memec/
done
