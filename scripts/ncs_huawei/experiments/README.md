# Setup
- `node11`-`node23`, `node37`-`node39` (16): Servers
- `node1` (1): Coordinator
- `node2`-`node9` (8): Clients / YCSB

# Screens
- `node1`: `coordinator`
- `node11`-`node23`, `node37`-`node39`: `slave`
- `node2`-`node9`: `master` & `ycsb`
- `node10`: `experiment` & `manage`

# Scripts
- `control`: For executing YCSB clients on each of the client nodes
- `master`: For executing one YCSB client locally
