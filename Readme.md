
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
sudo apt-get install automake yasm libtool
```

Check if `yasm` is of version 1.2 or above,
```
yasm --version
```

If not, install yasm from source,
```
wget http://www.tortall.net/projects/yasm/releases/yasm-1.2.0.tar.gz
tar zxvf yasm-1.2.0.tar.gz
cd yasm-1.2.0
make
sudo make install
cd ../
```

Check if `automake` is of version 1.14 or above,
```
automake --version
```

If not, install automake from source,
```
wget http://ftp.gnu.org/gnu/automake/automake-1.14.tar.gz
tar zxf automake-1.14.tar.gz
cd automake-1.14
make
sudo make install
cd ../
```

Check if `autoconf` is of version 2.69 or above,
```
autoconf --version
```

If not, install autoconf from source,
```
wget http://ftp.gnu.org/gnu/autoconf/autoconf-2.69.tar.gz
tar zxf autoconf-2.69.tar.gz
cd autoconf-2.69
make
sudo make install
cd ../
```

Configure and compile ISA-L under `lib/isa-l-2.14.0/`,

```
./configure --prefix=$(pwd)
make install
```

If you encounter the error "Libtool library used but 'LIBTOOL' is undefined",

1. Add `AC_CONFIG_MACRO_DIR([m4])` to `configure.ac`
2. Add libtool support: `$ libtoolize`
3. Continue to install: `$ ./configure --prefix=$(pwd); make install`

The system uses Jerasure for coding by default (for RS and CRS), set `USE_ISAL` in `Makefile` to `1` to use ISA-L instead
```
USE_ISAL= 1
``` 
