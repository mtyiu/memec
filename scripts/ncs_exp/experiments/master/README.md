# Scripts for running experiments

## Experiments
### 1. Baseline performance against Redis and Memcached, as well as Tachyon
- Scripts: `workloads-*.sh`

### 2. Encoding overhead 
- Scripts: `encoding.sh` and `workload-redis.sh` (with replication in Redis enabled)

### 3. Degraded SET
- Scripts: `degraded.sh`

### 4. Degraded GET and UPDATE
- Scripts: `degraded.sh`

### 5. Degraded performance
- Scripts: `temporary*.sh`

### 6. Recovery
- Scripts: `recovery.sh`

### 7. Memory Utilization
- Scripts: `memory.sh`

## Others
### Compare YCSB performance under different number of threads
- Script: `thread_count.sh`
