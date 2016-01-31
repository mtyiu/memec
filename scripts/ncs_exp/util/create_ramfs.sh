#!/bin/bash

BASE_PATH=${HOME}/mtyiu
BOOTSTRAP_SCRIPT_PATH=${BASE_PATH}/scripts/bootstrap

SIZE="4g"
TARGET_DIR="/tmp/plio"

rm -f .job

for i in {11..23} {37..39}; do
	echo "ssh testbed-node$i \"if [ -d ${TARGET_DIR} ]; then echo \"[Node $i] Storage directory exists.\"; else echo \"[Node$i] Create storage directory ${TARGET_DIR}\"; sudo mkdir -p ${TARGET_DIR}; sudo mount -t ramfs -o size=${SIZE} ramfs ${TARGET_DIR}; fi\"" >> .job
done

parallel -j3 < .job

rm -f .job
