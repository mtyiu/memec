#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <arpa/inet.h>
#include "server_addr.hh"

ServerAddr::ServerAddr() {
	this->name = NULL;
}

ServerAddr::ServerAddr( const char *name, unsigned long addr, unsigned short port, int type ) {
	this->name = strdup( name );
	this->addr = addr;
	this->port = port;
	this->type = type;
}

bool ServerAddr::parse( const char *name, const char *addr ) {
	char *s, ip[ 16 ];
	struct in_addr inAddr;

	// Read protocol
	if ( strncmp( addr, "tcp://", 6 ) == 0 ) {
		this->type = SOCK_STREAM;
	} else if ( strncmp( addr, "udp://", 6 ) == 0 ) {
		this->type = SOCK_DGRAM;
	} else {
		return false;
	}

	// Read port number
	s = strrchr( ( char * ) addr, ':' );
	if ( s == NULL )
		return false;
	s++;
	if ( sscanf( s, "%hu", &this->port ) != 1 ) {
		return false;
	}
	this->port = htons( this->port );

	// Read IP address
	strncpy( ip, addr + 6, s - addr - 7 );
	ip[ s - addr - 7 ] = 0;
	inet_pton( AF_INET, ip, &inAddr );
	this->addr = inAddr.s_addr;

	this->name = strdup( name );
	return true;
}

void ServerAddr::print() {
	struct in_addr addr;
	char buf[ INET_ADDRSTRLEN ];
	if ( this->name ) {
		addr.s_addr = this->addr;
		inet_ntop( AF_INET, &addr, buf, INET_ADDRSTRLEN );
		printf(
			"[%s] %s://%s:%d\n",
			this->name,
			this->type == SOCK_STREAM ? "tcp" : "udp",
			buf,
			ntohs( this->port )
		);
	}
}

ServerAddr::~ServerAddr() {
	if ( this->name )
		free( this->name );
}
