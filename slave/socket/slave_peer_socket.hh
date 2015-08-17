#ifndef __SLAVE_SOCKET_SLAVE_PEER_SOCKET_HH__
#define __SLAVE_SOCKET_SLAVE_PEER_SOCKET_HH__

#include "../../common/socket/socket.hh"

class SlavePeerSocket : public Socket {
private:
	bool active;
	bool received;
	struct sockaddr_in recvAddr;
	char *identifier;
	EPoll *epoll;

	void registerTo();

public:
	bool registered;
	bool self;

	SlavePeerSocket();
	bool init( ServerAddr &addr, EPoll *epoll, bool active, bool self );
	bool start();
	void stop();
	bool ready();
	void free();
	bool isMatched( ServerAddr &serverAddr );
	bool setRecvFd( int fd, struct sockaddr_in *addr );
	ssize_t send( char *buf, size_t ulen, bool &connected );
	ssize_t recv( char *buf, size_t ulen, bool &connected, bool wait );
	void print( FILE *f = stdout );
};

#endif
