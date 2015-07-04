#ifndef __SOCKET_HH__
#define __SOCKET_HH__

#ifndef SOL_TCP
#define SOL_TCP IPPROTO_TCP
#endif

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
	struct sockaddr_in addr;

	bool setSockOpt( int level, int optionName );
	inline bool setReuse();
	inline bool setNoDelay();
	inline bool setNonBlocking();
	
	bool listen();
	bool connect();
	ssize_t send( int sockfd, char *buf, size_t ulen, bool &connected );
	ssize_t recv( int sockfd, char *buf, size_t ulen, bool &connected, bool wait );

public:
	bool init( int type, unsigned long addr, unsigned short port );
	virtual bool start() = 0;
	virtual void stop();

	// Utilities
	bool hton_ip( char *ip, unsigned long &ret );
	bool hton_port( char *port, unsigned short &ret );
	unsigned short hton_port( unsigned short port );

	bool ntoh_ip( unsigned long ip, char *buf, size_t len );
	bool ntoh_port( unsigned short port, char *buf, size_t len );
	unsigned short ntoh_port( unsigned short port );
};

#endif
