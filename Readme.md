
Installation
====

Spread Library
---
For Ubuntu 12.04LTS:

```
sudo apt-get install adduser spread libspread1 libspread1-dev
```
For Ubuntu 13.x - 15.x:

Search and download the following packages from [http://packages.ubuntu.com/](http://packages.ubuntu.com/):
```
adduser, spread, libspread1, libspread1-dev
```

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

Check if `yasm` is of version 1.2 or above,
```
yasm --version
```

If not, install yasm from source,
```
wget http://www.tortall.net/projects/yasm/releases/yasm-1.2.0.tar.gz
tar zxvf yasm-1.2.0.tar.gz
make
sudo make install
```

Configure under `lib/isa-l-2.14.0/`,

```
./configure --prefix=$(pwd)
make install
```

If you encounter the error "Libtool library used but 'LIBTOOL' is undefined",

1. Install `libtool`: `$ sudo apt-get install libtool`
2. Add `AC_CONFIG_MACRO_DIR([m4])` to `configure.ac`
3. Add libtool support: `$ libtoolize`
4. Continue to install: `$ ./configure --prefix=$(pwd); make install`

The system uses Jerasure for coding by default (for RS and CRS), set `USE_ISAL` in `Makefile` to `1` to use ISA-L instead
```
USE_ISAL= 1
``` 
