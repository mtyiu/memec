#!/bin/bash

BASE_DIR=${HOME}/mtyiu

rm -rf ${BASE_DIR}/results
rm ${BASE_DIR}/scripts

mkdir ${BASE_DIR}/results
ln -s ${BASE_DIR}/plio/scripts/ncs/ ${BASE_DIR}/scripts
