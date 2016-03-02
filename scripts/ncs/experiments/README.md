# Setup
- `node11`-`node23`, `node37`-`node39` (15): Servers
- `node1` (1): Coordinator
- `node2`, `node5`, `node7` (3): Backup servers
- `node3`, `node4`, `node8`, `node9` (4): Clients / YCSB

# Screens
- `node1`: `coordinator`
- `node11`-`node23`, `node37`-`node39`: `slave` & `ethtool`
- `node3`, `node4`, `node8`, `node9`: `master` & `ycsb`
- `node10`: `experiment` & `manage`

# Scripts
- `control`: For executing YCSB clients on each of the client nodes
- `master`: For executing one YCSB client locally

# Guide to run an experiment
1. Start the screens on all corresponding nodes
2. Mount a Ramfs on each slave under `/tmp/plio`or just create the folder on each slave
3. Set proper configurations, esp. when switching between control and real experiments
    - The scripts use configuration files under `bin/config/ncs/` by default
    - In `global.ini`,
        - Encoding scheme under `coding`
        - Encoding parameters under the names of coding schemes
        - Enable degraded mode by setting 1 for `remap > enabled`
        - Enable manual transition by setting 1 for `remap > enabled`
    - In `coordinator.ini`, `client.ini`, and `server.ini`,
        - Tune the number of worker threads under `workers`
    - In `client.ini`,
        - Tune the number of acks to batch before acknowledging parity delta backups under `backup > ackBatchSize`
    - In `server.ini`,
        - Tune the storage capacity of each slave under `pool > chunks` (in bytes)
4. Modify the `BASE_PATH` and `PLIO_PATH` in the scripts to the folder containing the scripts, and the path to PLIO folder respectively.
5. Synchronize the scripts, together with PLIO components, to all the nodes (we assume a homogenous setting, i.e., same `BASE_PATH` and `PLIO_PATH`).
6. Run an experiment script in the screen `experiment` on `node10`

# Notes to scripts execution
- Control flow: `control` >  `../bootstrap`, `master`, or `../util` > `../ycsb/`
- Each script driving a YCSB or Java client (under `master`) sends a "End of line" character to the `experiment` screen before exits.
- Each script driving the whole experiment (under `control`) copies results from masters to a designated folder. Change the destination in the script between executions to avoid accidental overwrites.
