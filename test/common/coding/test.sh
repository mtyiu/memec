#!/bin/bash

coding=("raid5" "rdp" "cauchy" "rs")
k=("4" "6" "8" "12")
csizes=("2048" "4096" "8192" "16384")
rounds=5000
OUT="./coding.perf.out"

sed -i "s/\(ROUNDS (\).*)/\1${rounds})/" performance.cc
echo "ROUNDS=${rounds}" | tee ${OUT}

for s in ${csizes[@]}; do
    sed -i "s/\(CHUNK_SIZE (\).*)/\1${s})/" performance.cc
    for i in ${k[@]}; do
        sed -i "s/\(C_K (\).*)/\1${i})/" performance.cc
        make
        for c in ${coding[@]}; do
            echo "======== ${s},${i},${c} ==========" | tee -a ${OUT}
            ./performance ${c} 2>&1 | tee -a ${OUT}
            sleep 5
        done
    done
done
