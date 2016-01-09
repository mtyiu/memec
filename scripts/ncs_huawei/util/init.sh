#!/bin/bash

BASE_PATH=${HOME}/mtyiu

rm -rf ${BASE_PATH}/results
rm ${BASE_PATH}/scripts_huawei

mkdir ${BASE_PATH}/results
ln -s ${BASE_PATH}/plio/scripts/ncs_huawei/ ${BASE_PATH}/scripts_huawei
