#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio

cd ${PLIO_PATH}/test/setter

threads=64
size=$1
field_length=4050

echo "Running load phase for size = $size..."

# ${BASE_PATH}/scripts/ycsb/plio/load-custom.sh $threads $size $field_length
time java -cp . edu.cuhk.cse.plio.Main 255 4096 $(hostname -I | xargs) 9112 $size 30 4060 64

# Tell the control node that this iteration is finished
ssh testbed-node10 "screen -S experiment -p 0 -X stuff \"$(printf '\r')\""

echo "Finished load phase for size = $size..."
