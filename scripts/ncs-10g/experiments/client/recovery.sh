#!/bin/bash

BASE_PATH=${HOME}/mtyiu
MEMEC_PATH=${BASE_PATH}/memec

cd ${MEMEC_PATH}/test/setter
# cd ${MEMEC_PATH}/test/setget

size=$1

echo "Running load phase for size = $size..."

# ${MEMEC_PATH}/scripts/ncs-10g/ycsb/memec/load-custom.sh $threads $size $field_length
time java -cp . edu.cuhk.cse.memec.Main 255 4096 $(hostname -I | sed 's/^.*\(192\.168\.10\.[0-9]*\).*$/\1/g') 9112 $size 30 4060 64
# time ./test 255 4096 4080 $size 32 false $(hostname -I | sed 's/^.*\(192\.168\.10\.[0-9]*\).*$/\1/g') 9112

# Tell the control node that this iteration is finished
ssh testbed-node10 "screen -S experiment -p 0 -X stuff \"$(printf '\r')\""

echo "Finished load phase for size = $size..."
