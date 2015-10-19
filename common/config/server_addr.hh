#ifndef __COMMON_CONFIG_SERVER_ADDR_HH__
#define __COMMON_CONFIG_SERVER_ADDR_HH__

#include <cstdio>
#include <stdint.h>

#define SERVER_NAME_MAX_LEN				255
#define SERVER_ADDR_MESSSAGE_MAX_LEN	SERVER_NAME_MAX_LEN + 1 + 4 + 2

class ServerAddr {
private:
	bool initialized;

public:
	char name[ SERVER_NAME_MAX_LEN + 1 ];
	uint32_t id;
	uint32_t addr;
	uint16_t port;
	int type;

	ServerAddr();
	ServerAddr( const char *name, uint32_t addr, uint16_t port, int type );
	bool isInitialized();
	bool parse( const char *name, const char *addr );
	size_t serialize( char *message );
	size_t deserialize( const char *message );
	void print( FILE *f = stdout );
	bool operator==( const ServerAddr &addr ) const;
	static bool match( const ServerAddr *a1, const ServerAddr *a2 );
};

#endif
