#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <arpa/inet.h>
#include "server_addr.hh"

ServerAddr::ServerAddr() {
	this->initialized = false;
	this->name[ 0 ] = 0;
	this->name[ SERVER_NAME_MAX_LEN ] = 0;
}

ServerAddr::ServerAddr( const char *name, uint32_t addr, uint16_t port, int type ) {
	this->initialized = true;
	if ( name != NULL ) 
		strncpy( this->name, name, SERVER_NAME_MAX_LEN );
	else
		this->name[ 0 ] = 0;
	this->addr = addr;
	this->port = port;
	this->type = type;
}

bool ServerAddr::isInitialized() {
	return this->initialized;
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

	strncpy( this->name, name, SERVER_NAME_MAX_LEN );
	this->initialized = true;
	return true;
}

size_t ServerAddr::serialize( char *message ) {
	*( ( uint32_t * )( message     ) ) = htonl( this->addr );
	*( ( uint16_t * )( message + 4 ) ) = htons( this->port );
	strcpy( message + 6, this->name );
	return 6 + strlen( this->name ) + 1;
}

size_t ServerAddr::deserialize( const char *message ) {
	this->addr = ntohl( *( ( uint32_t * )( message     ) ) );
	this->port = ntohs( *( ( uint16_t * )( message + 4 ) ) );
	strncpy( this->name, message + 6, SERVER_NAME_MAX_LEN + 1 );
	return 6 + strlen( this->name ) + 1;
}

void ServerAddr::print( FILE *f ) {
	if ( ! this->initialized ) {
		fprintf( f, "(nil)\n" );
		return;
	}

	struct in_addr addr;
	char buf[ INET_ADDRSTRLEN ];
	addr.s_addr = this->addr;
	inet_ntop( AF_INET, &addr, buf, INET_ADDRSTRLEN );
	fprintf(
		f,
		"[%s] %s://%s:%d\n",
		this->name,
		this->type == SOCK_STREAM ? "tcp" : "udp",
		buf,
		ntohs( this->port )
	);
}

bool ServerAddr::operator==( const ServerAddr &addr ) const {
	return (
		strncmp( this->name, addr.name, SERVER_NAME_MAX_LEN ) == 0 &&
		this->addr == addr.addr &&
		this->port == addr.port &&
		this->type == addr.type
	);
}
