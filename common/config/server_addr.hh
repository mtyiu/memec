#ifndef __SERVER_ADDR_HH__
#define __SERVER_ADDR_HH__

#include <cstdio>

#define SERVER_NAME_MAX_LEN	255

class ServerAddr {
private:
	bool initialized;

public:
	char name[ SERVER_NAME_MAX_LEN + 1 ];
	unsigned long addr;
	unsigned short port;
	int type;

	ServerAddr();
	ServerAddr( const char *name, unsigned long addr, unsigned short port, int type );
	bool isInitialized();
	bool parse( const char *name, const char *addr );
	void print( FILE *f = stdout );
	bool operator==( const ServerAddr &addr ) const;
};

#endif
