#!/bin/bash

BASE_PATH=${HOME}/mtyiu
PLIO_PATH=${BASE_PATH}/plio

cd ${PLIO_PATH}/test/setter

size=$1

echo "Running load phase for size = $size..."
time java -cp . edu.cuhk.cse.plio.Main 255 4096 $(hostname -I | xargs) 9112 $size 64 true

# Tell the control node that this iteration is finished
ssh testbed-node10 "screen -S experiment -p 0 -X stuff \"$(printf '\r')\""

echo "Finished load phase for size = $s..."
