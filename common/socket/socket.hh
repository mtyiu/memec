#ifndef __COMMON_SOCKET_SOCKET_HH__
#define __COMMON_SOCKET_SOCKET_HH__

#ifndef SOL_TCP
#define SOL_TCP IPPROTO_TCP
#endif

#include <cstdio>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

enum SocketMode {
	SOCKET_MODE_UNDEFINED,
	SOCKET_MODE_LISTEN,
	SOCKET_MODE_CONNECT
};

class Socket {
protected:
	SocketMode mode;
	int sockfd;
	bool connected;
	struct sockaddr_in addr;

	bool setSockOpt( int level, int optionName );
	inline bool setReuse();
	inline bool setNoDelay();
	inline bool setNonBlocking();

	bool listen();
	bool connect();
	ssize_t send( int sockfd, char *buf, size_t ulen, bool &connected );
	ssize_t recv( int sockfd, char *buf, size_t ulen, bool &connected, bool wait = false );
	int accept( struct sockaddr_in *addrPtr = 0, socklen_t *addrlenPtr = 0 );

public:
	bool init( int type, unsigned long addr, unsigned short port );
	bool init( int sockfd, struct sockaddr_in addr );
	inline int getSocket() {
		return this->sockfd;
	}
	virtual bool start() = 0;
	virtual void stop();
	void print( FILE *f = stdout );

	// Utilities
	static bool hton_ip( char *ip, unsigned long &ret );
	static bool hton_port( char *port, unsigned short &ret );
	static unsigned short hton_port( unsigned short port );

	static bool ntoh_ip( unsigned long ip, char *buf, size_t len );
	static bool ntoh_port( unsigned short port, char *buf, size_t len );
	static unsigned short ntoh_port( unsigned short port );
};

#endif
