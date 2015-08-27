#ifndef __COMMON_SOCKET_SOCKET_HH__
#define __COMMON_SOCKET_SOCKET_HH__

#ifndef SOL_TCP
#define SOL_TCP IPPROTO_TCP
#endif

#include <cstdio>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "epoll.hh"
#include "../config/server_addr.hh"

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
	struct {
		char data[ 4096 ];
		size_t size;
		pthread_mutex_t lock;
	} pending;

	bool setSockOpt( int level, int optionName );
	bool setReuse();
	bool setNoDelay();
	bool setNonBlocking();

	bool listen();
	bool connect();
	ssize_t send( int sockfd, char *buf, size_t ulen, bool &connected );
	ssize_t recv( int sockfd, char *buf, size_t ulen, bool &connected, bool wait = false );
	int accept( struct sockaddr_in *addrPtr = 0, socklen_t *addrlenPtr = 0 );

public:
	inline int getSocket() {
		return this->sockfd;
	}
	Socket();
	bool init( int type, unsigned long addr, unsigned short port, bool block = false );
	bool init( int sockfd, struct sockaddr_in addr );
	virtual bool init( ServerAddr &addr, EPoll *epoll );
	virtual bool start() = 0;
	virtual void stop();
	virtual bool ready();
	virtual void print( FILE *f = stdout );
	void printAddress( FILE *f = stdout );
	struct sockaddr_in getAddr();
	bool equal( Socket &s );
	bool equal( unsigned long addr, unsigned short port );

	// Utilities
	static bool setNonBlocking( int fd );
	static bool hton_ip( char *ip, unsigned long &ret );
	static bool hton_port( char *port, unsigned short &ret );
	static unsigned short hton_port( unsigned short port );

	static bool ntoh_ip( unsigned long ip, char *buf, size_t len );
	static bool ntoh_port( unsigned short port, char *buf, size_t len );
	static unsigned short ntoh_port( unsigned short port );
};

#endif
