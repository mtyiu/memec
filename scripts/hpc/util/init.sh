#!/bin/bash

BASE_PATH=${HOME}/mtyiu

rm -rf ${BASE_PATH}/results
rm ${BASE_PATH}/scripts

mkdir ${BASE_PATH}/results
ln -s ${BASE_PATH}/memec/scripts/hpc/ ${BASE_PATH}/scripts
