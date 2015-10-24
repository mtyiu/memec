#ifndef __COMMON_SOCKET_SOCKET_HH__
#define __COMMON_SOCKET_SOCKET_HH__

#ifndef SOL_TCP
#define SOL_TCP IPPROTO_TCP
#endif

#include <cstdio>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "epoll.hh"
#include "../config/server_addr.hh"
#include "../lock/lock.hh"

enum SocketMode {
	SOCKET_MODE_UNDEFINED,
	SOCKET_MODE_LISTEN,
	SOCKET_MODE_CONNECT
};

class Socket {
protected:
	bool connected;
	SocketMode mode;
	int sockfd;
	int type;
	struct sockaddr_in addr;
	LOCK_T readLock, writeLock;

	static EPoll *epoll;

	bool setSockOpt( int level, int optionName );
	bool setReuse();
	bool setNoDelay();
	bool setNonBlocking();

	bool listen();
	bool connect();
	ssize_t send( int sockfd, char *buf, size_t ulen, bool &connected );
	ssize_t recv( int sockfd, char *buf, size_t ulen, bool &connected, bool wait = false );
	ssize_t recvRem( int sockfd, char *buf, size_t ulen, char *prevBuf, size_t prevSize, bool &connected );
	bool done( int sockfd );
	int accept( struct sockaddr_in *addrPtr = 0, socklen_t *addrlenPtr = 0 );

public:
	inline int getSocket() {
		return this->sockfd;
	}
	static void init( EPoll *epoll );
	Socket();
	bool init( int type, uint32_t addr, uint16_t port, bool block = false );
	bool init( int sockfd, struct sockaddr_in addr );
	virtual bool init( ServerAddr &addr, EPoll *epoll );
	virtual bool start() = 0;
	virtual void stop();
	virtual bool ready();
	virtual void print( FILE *f = stdout );
	void printAddress( FILE *f = stdout );
	struct sockaddr_in getAddr();
	ServerAddr getServerAddr();
	bool equal( Socket *s );
	bool equal( uint32_t addr, uint16_t port );
	virtual ~Socket();

	// Utilities
	static bool setNonBlocking( int fd );
	static bool hton_ip( char *ip, uint32_t &ret );
	static bool hton_port( char *port, uint16_t &ret );
	static uint16_t hton_port( uint16_t port );

	static bool ntoh_ip( uint32_t ip, char *buf, size_t len );
	static bool ntoh_port( uint16_t port, char *buf, size_t len );
	static uint16_t ntoh_port( uint16_t port );
};

#endif
