
Installation
====

Spread Library
---
For Ubuntu 12.04LTS:

```
sudo apt-get install adduser spread libspread1 libspread1-dev
```
For Ubuntu 13.x - 15.x:

Search and download the following libraries from [http://packages.ubuntu.com/](http://packages.ubuntu.com/):
adduser, spread, libspread1, libspread1-dev

Manually install the packages,

```
sudo dpkg -i adduser*.deb spread*.deb libspread1*.deb
```

Intel Storage Accelerational Library (Open source version), ISA-L
---

Install packages,

```
sudo apt-get install automake yasm 
```

Configure under `lib/isa-l-2.14.0/`,

```
./configure

```

The system uses Jerasure for coding by default (for RS and CRS), uncomment `-DUSE_ISAL` in `common/coding/Makefile` to use ISA-L instead
```
FLAGS= -DUSE_ISAL
``` 
