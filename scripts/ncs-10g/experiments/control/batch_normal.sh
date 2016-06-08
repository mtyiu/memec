#!/bin/bash

./workloads-memec.sh
echo "./workloads-memec.sh" > RUNNING

./workloads-redis.sh
echo "./workloads-redis.sh" > RUNNING

./workloads-memcached.sh
echo "./workloads-memcached.sh" > RUNNING

./encoding-redis.sh
echo "./encoding-redis.sh" > RUNNING

./encoding.sh
echo "./encoding.sh" > RUNNING
