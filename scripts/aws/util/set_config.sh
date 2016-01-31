#!/bin/bash

## path to config files
PLIO_ROOT=/home/ubuntu/mtyiu/plio
CONFIG_DIR=${PLIO_ROOT}/bin/config/aws
SCRIPT_DIR=/home/ubuntu/mtyiu/scripts/util

## configurations
scheme=raid0
stats_interval=50
remap=1
start_th=20
stop_th=12
overload_th=130

## make the changes to specific files
sed -i "s/\(updateInterval=\).*/\1${stats_interval}/" ${CONFIG_DIR}/master.ini
sed -i "s/\(updateInterval=\).*/\1${stats_interval}/" ${CONFIG_DIR}/master.ini

sed -i "s/\(scheme=\).*/\1${scheme}/" ${CONFIG_DIR}/global.ini
sed -i "s/\(enabled=\).*/\1${remap}/" ${CONFIG_DIR}/global.ini
sed -i "s/\(startThreshold=\).*/\1${start_th}/" ${CONFIG_DIR}/global.ini
sed -i "s/\(stopThreshold=\).*/\1${stop_th}/" ${CONFIG_DIR}/global.ini
sed -i "s/\(overloadThreshold=\).*/\1${overload_th}/" ${CONFIG_DIR}/global.ini

## rsync the configuration
${SCRIPT_DIR}/rsync.sh
