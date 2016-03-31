#!/bin/bash

./degraded_set_part1.sh
echo "./degraded_set_part1.sh" > RUNNING

./degraded_set_part2.sh
echo "./degraded_set_part2.sh" > RUNNING

./degraded-a.sh
echo "./degraded-a.sh" > RUNNING

./degraded-c.sh
echo "./degraded-c.sh" > RUNNING

./temporary_control.sh
echo "./temporary_control.sh" > RUNNING

./temporary_control-double.sh
echo "./temporary_control-double.sh" > RUNNING
