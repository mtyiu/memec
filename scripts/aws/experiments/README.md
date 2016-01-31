# Setup
- `node1`-`node10` (10): Servers
- `coordinator` (localhost) (1): Coordinator
- `node11`, `node12` (3): Backup servers
- `client` (1): Clients / YCSB

# Screens
- `coordinator`: `coordinator`, `control`, `experiment`
- `node1`-`node12`: `slave`
- `client`: `master[1-4]` & `ycsb[1-4]`

# Scripts
- `control`: For executing YCSB clients on each of the client nodes
- `master`: For executing one YCSB client locally
