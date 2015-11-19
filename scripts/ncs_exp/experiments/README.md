# Setup
- `node11`-`node23`, `node37`-`node39` (15): Servers
- `node1` (1): Coordinator
- `node2`, `node5`, `node7` (3): Backup servers
- `node3`, `node4`, `node8`, `node9` (4): Clients / YCSB

# Screens
- `node1`: `coordinator`
- `node11`-`node23`, `node37`-`node39`: `slave`
- `node3`, `node4`, `node8`, `node9`: `master` & `ycsb`
- `node10`: `experiment` & `manage`

# Scripts
- `control`: For executing YCSB clients on each of the client nodes
- `master`: For executing one YCSB client locally
