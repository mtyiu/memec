#!/bin/bash

for i in {1..10}; do
	rsync \
		--delete \
		--force \
		--progress \
		--verbose \
		--archive \
		~/mtyiu/memec/ node$i:mtyiu/memec/
done

rsync \
	--delete \
	--force \
	--progress \
	--verbose \
	--archive \
	~/mtyiu/memec/ client:mtyiu/memec/
