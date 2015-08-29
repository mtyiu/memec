#include <cstdio>
#include <cstring>
#include <cerrno>
#include <climits>
#include <fcntl.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include "socket.hh"
#include "../util/debug.hh"

EPoll *Socket::epoll;

bool Socket::setSockOpt( int level, int optionName ) {
	int optionValue = 1;
	socklen_t optionLength = sizeof( optionValue );
	if ( setsockopt( this->sockfd, level, optionName, &optionValue, optionLength ) == -1 ) {
		__ERROR__( "Socket", "setSockOpt", "%s", strerror( errno ) );
		return false;
	}
	return true;
}

bool Socket::setReuse() {
	return this->setSockOpt( SOL_SOCKET, SO_REUSEADDR );
}

bool Socket::setNoDelay() {
	return this->setSockOpt( SOL_TCP, TCP_NODELAY );
}

bool Socket::setNonBlocking() {
	return Socket::setNonBlocking( this->sockfd );
}

bool Socket::setNonBlocking( int fd ) {
	int flags;

	flags = fcntl( fd, F_GETFL, 0 );
	if ( flags == -1 ) {
		__ERROR__( "Socket", "setNonBlocking", "fcntl(): %s", strerror( errno ) );
		return false;
	}

	flags |= O_NONBLOCK;
	if ( fcntl( fd, F_SETFL, flags ) == -1 ) {
		__ERROR__( "Socket", "setNonBlocking", "fcntl(): %s", strerror( errno ) );
		return false;
	}

	return true;
}

bool Socket::listen() {
	if ( this->mode != SOCKET_MODE_UNDEFINED ) {
		__ERROR__( "Socket", "listen", "This mode of this socket is set before." );
		return false;
	}

	if ( ::bind( this->sockfd, ( struct sockaddr * ) &this->addr, sizeof( this->addr ) ) != 0 ||
	     ::listen( this->sockfd, 20 ) != 0 ) {
		__ERROR__( "Socket", "listen", "%s", strerror( errno ) );
		return false;
	}

	this->mode = SOCKET_MODE_LISTEN;
	this->connected = true;
	return true;
}

bool Socket::connect() {
	if ( this->mode != SOCKET_MODE_UNDEFINED ) {
		__ERROR__( "Socket", "connect", "This mode of this socket is set before." );
		return false;
	}

	if ( ::connect( this->sockfd, ( struct sockaddr * ) &this->addr, sizeof( this->addr ) ) != 0 ) {
		this->connected = false;
		__ERROR__( "Socket", "connect", "%s", strerror( errno ) );
		return false;
	}

	this->mode = SOCKET_MODE_CONNECT;
	this->connected = true;
	return this->setNonBlocking();
}

ssize_t Socket::send( int sockfd, char *buf, size_t ulen, bool &connected ) {
	ssize_t ret = 0, bytes = 0, len = ulen;
	do {
		ret = ::send( sockfd, buf + bytes, len - bytes, 0 );
		if ( ret == -1 ) {
			if ( errno == EWOULDBLOCK ) {
				continue;
			}
			__ERROR__( "Socket", "send", "%s", strerror( errno ) );
			connected = false;
			return -1;
		} else if ( ret == 0 ) {
			connected = false;
			break;
		} else {
			connected = true;
			bytes += ret;
		}
	} while ( bytes < len );
	// if ( connected && bytes > 0 )
	// 	__DEBUG__( MAGENTA, "Socket", "send", "Sent %ld bytes.", bytes );
	this->connected = connected;
	return bytes;
}

ssize_t Socket::recv( int sockfd, char *buf, size_t ulen, bool &connected, bool wait ) {
	ssize_t ret = 0, bytes = 0, len = ulen;
	do {
		ret = ::recv( sockfd, buf + bytes, len - bytes, 0 );
		if ( ret == -1 ) {
			if ( errno != EAGAIN ) {
				__ERROR__( "Socket", "recv", "[%d] %s", this->sockfd, strerror( errno ) );
				connected = false;
				break;
			} else {
				connected = true;
				if ( ! wait ) break;
			}
		} else if ( ret == 0 ) {
			connected = false;
			break;
		} else {
			connected = true;
			bytes += ret;
		}
	} while ( bytes < len );
	// if ( connected && bytes > 0 )
	// 	__DEBUG__( MAGENTA, "Socket", "recv", "Received %ld bytes.", bytes );
	this->connected = connected;
	return bytes;
}

ssize_t Socket::recvRem( int sockfd, char *buf, size_t expected, char *prevBuf, size_t prevSize, bool &connected ) {
	ssize_t bytes;
	if ( buf != prevBuf )
		memmove( buf, prevBuf, prevSize );
	buf += prevSize;
	expected -= prevSize;
	bytes = prevSize;
	bytes += this->recv( sockfd, buf, expected, connected, true );
	return bytes;
}

bool Socket::done( int sockfd ) {
	return Socket::epoll->modify( sockfd, EPOLL_EVENT_SET );
}

int Socket::accept( struct sockaddr_in *addrPtr, socklen_t *addrlenPtr ) {
	struct sockaddr_in addr;
	socklen_t addrlen;
	int ret;

	addrlen = sizeof( addr );
	ret = ::accept( this->sockfd, ( struct sockaddr * ) &addr, &addrlen );
	if ( ret == -1 ) {
		if ( errno != EAGAIN && errno != EWOULDBLOCK ) {
			__ERROR__( "IOServer", "accept", "%s", strerror( errno ) );
		}
		return -1;
	} else {
		if ( addrPtr && addrlenPtr ) {
			memcpy( addrPtr, &addr, sizeof( addr ) );
			memcpy( addrlenPtr, &addrlen, sizeof( addrlen ) );
		}
		return ret;
	}
}

Socket::Socket() {
	this->pending.size = 0;
	pthread_mutex_init( &this->pending.lock, 0 );
}

void Socket::init( EPoll *epoll ) {
	Socket::epoll = epoll;
}

bool Socket::init( int type, uint32_t addr, uint16_t port, bool block ) {
	this->mode = SOCKET_MODE_UNDEFINED;
	this->connected = false;
	this->sockfd = socket( AF_INET, type, 0 );
	if ( this->sockfd < 0 ) {
		__ERROR__( "Socket", "init", "%s", strerror( errno ) );
		return false;
	}
	this->type = type;
	memset( &this->addr, 0, sizeof( this->addr ) );
	this->addr.sin_family = AF_INET;
	this->addr.sin_port = port;
	this->addr.sin_addr.s_addr = addr;

	if ( ! block ) {
		if ( ! this->setNonBlocking() )
			return false;
	}

	return (
		this->setReuse() &&
		this->setNoDelay()
	);
}

bool Socket::init( ServerAddr &addr, EPoll *epoll ) {
	if ( this->init( addr.type, addr.addr, addr.port, true ) ) {
		if ( epoll )
			epoll->add( this->sockfd, EPOLL_EVENT_SET );
		return true;
	}
	return false;
}

bool Socket::init( int sockfd, struct sockaddr_in addr ) {
	this->mode = SOCKET_MODE_CONNECT;
	this->sockfd = sockfd;
	this->addr = addr;
	this->connected = true;
	return true;
}

void Socket::stop() {
	::close( this->sockfd );
	this->connected = false;
}

bool Socket::ready() {
	return this->connected;
}

void Socket::print( FILE *f ) {
	char buf[ 16 ];
	Socket::ntoh_ip( this->addr.sin_addr.s_addr, buf, 16 );
	fprintf( f, "[%4d] %s:%u (%sconnected)\n", this->sockfd, buf, Socket::ntoh_port( this->addr.sin_port ), this->connected ? "" : "dis" );
}

void Socket::printAddress( FILE *f ) {
	char buf[ 16 ];
	Socket::ntoh_ip( this->addr.sin_addr.s_addr, buf, 16 );
	fprintf( f, "%s:%u", buf, Socket::ntoh_port( this->addr.sin_port ) );
}

struct sockaddr_in Socket::getAddr() {
	return this->addr;
}

bool Socket::equal( Socket &s ) {
	struct sockaddr_in saddr = s.getAddr();
	return this->equal( saddr.sin_addr.s_addr, saddr.sin_port );
}

bool Socket::equal( uint32_t addr, uint16_t port ) {
	return (
		this->addr.sin_port == port &&
		this->addr.sin_addr.s_addr == addr
	);
}

bool Socket::hton_ip( char *ip, uint32_t &ret ) {
	struct in_addr addr;
	switch( inet_pton( AF_INET, ip, &addr ) ) {
		case 1:
			ret = addr.s_addr;
			return true;
		case 0:
			__ERROR__( "Socket", "hton_ip", "The address is not parseable in the specified address family." );
			return false;
		case -1:
		default:
			__ERROR__( "Socket", "hton_ip", "%s", strerror( errno ) );
			return false;
	}
}

bool Socket::hton_port( char *port, uint16_t &ret ) {
	int tmp;
	switch( sscanf( port, "%d", &tmp ) ) {
		case 1:
			if ( tmp < 0 || tmp > USHRT_MAX ) {
				__ERROR__( "Socket", "hton_port", "The port number is invalid." );
				return false;
			}
			ret = ( uint16_t ) tmp;
			ret = htons( ret );
			return true;
		case 0:
		default:
			__ERROR__( "Socket", "hton_port", "The port number cannot be converted into an uint16_t." );
			return false;
	}
}

uint16_t Socket::hton_port( uint16_t port ) {
	return htons( port );
}

bool Socket::ntoh_ip( uint32_t ip, char *buf, size_t len ) {
	struct in_addr addr;
	addr.s_addr = ip;
	if ( ! inet_ntop( AF_INET, &addr, buf, len ) ) {
		__ERROR__( "Socket", "ntoh_ip", "%s", strerror( errno ) );
		return false;
	}
	return true;
}

bool Socket::ntoh_port( uint16_t port, char *buf, size_t len ) {
	port = ntohs( port );
	if ( snprintf( buf, len, "%hu", port ) <= 0 ) {
		return false;
	}
	return true;
}

uint16_t Socket::ntoh_port( uint16_t port ) {
	return ntohs( port );
}
