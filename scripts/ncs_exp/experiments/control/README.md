#### Scripts for running experiments

### Basic flow of scripts:
The scripts we used is mostly variants of the following basic flow:

- for each delay setting,
    - for # of iterations to run,
        1. start plio components
        2. load the key-value pairs
        3. wait for each client to finish loading
        4. run the workload(s)
        5. wait for each client to finish running 
        6. gather results from failed server(s)

By overloading and restoring servers at proper stages, we generate scripts for different experiments.

### Experiments
We use YCSB to evalate performance in experiment 1-5, and Java clients (under `test/setter` and `test/memory`) in experiment 6-7. 

## 1. Baseline performance against Redis and Memcached, as well as Tachyon
- Scripts: `workloads-*.sh`
- Description: The script first load key-value pairs, and then run all workloads, as described by an [example use of YCSB](https://github.com/brianfrankcooper/YCSB/wiki/Core-Workloads).

## 2. Encoding overhead 
- Scripts: `encoding.sh` and `workload-redis.sh` (with replication in Redis enabled)
- Description: The script `encoding.sh` modifies `global.ini` to change the coding scheme. The values of *k* and *n* need to be set manually.

## 3. Degraded SET
- Scripts: `degraded_set_*.sh`
- Description: The scripts iterates over different delays specified. To run them as a control, disable degraded mode.

## 4. Degraded GET and UPDATE
- Scripts: `degraded*.sh`, excluding those used in the previous experiment
- Description: The scripts iterates over different delays specified. To run them as a control, disable degraded mode.

## 5. Degraded performance
- Scripts: `temporary*.sh`
- Description: The scripts are similar to that for degraded SET, GET, and UPDATE.

## 6. Recovery
- Scripts: `recovery*.sh`
- Description: The scripts iterates over different amount of data preloaded.

## 7. Memory Utilization
- Scripts: `memory*.sh`
- Description: The scripts iterates over different workloads, and changes the coding scheme to EVENODD by default.

### Others
## Compare YCSB performance under different number of threads
- Script: `thread_count.sh`

## Test and compare sensitivity to failure detection (not used)
- Script: `sensitivity.sh`

## Run scripts in batch
- Script: `batch_control.sh`
