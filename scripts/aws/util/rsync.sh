#!/bin/bash

for i in {1..10}; do
	rsync \
		--delete \
		--force \
		--progress \
		--verbose \
		--archive \
		~/mtyiu/plio/ node$i:mtyiu/plio/
done

rsync \
	--delete \
	--force \
	--progress \
	--verbose \
	--archive \
	~/mtyiu/plio/ client:mtyiu/plio/
