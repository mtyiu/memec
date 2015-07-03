#ifndef __SERVER_ADDR_HH__
#define __SERVER_ADDR_HH__

#include <cstdio>

#define SERVER_NAME_MAX_LEN	255

class ServerAddr {
public:
	char name[ SERVER_NAME_MAX_LEN + 1 ];
	unsigned long addr;
	unsigned short port;
	int type;

	ServerAddr();
	bool parse( const char *name, const char *addr );
	ServerAddr( const char *, unsigned long, unsigned short, int );
	void print( FILE *f = stdout );
};

#endif
