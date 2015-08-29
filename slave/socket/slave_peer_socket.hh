#ifndef __SLAVE_SOCKET_SLAVE_PEER_SOCKET_HH__
#define __SLAVE_SOCKET_SLAVE_PEER_SOCKET_HH__

#include "../../common/socket/socket.hh"

class SlavePeerSocket : public Socket {
private:
	bool received;
	struct sockaddr_in recvAddr;
	char *identifier;
	EPoll *epoll;

	void registerTo();

public:
	volatile bool registered;
	bool self;

	SlavePeerSocket();
	bool init( int tmpfd, ServerAddr &addr, EPoll *epoll, bool self );
	int init();
	bool start();
	void stop();
	bool ready();
	void free();
	bool setRecvFd( int fd, struct sockaddr_in *addr );
	ssize_t send( char *buf, size_t ulen, bool &connected );
	ssize_t recv( char *buf, size_t ulen, bool &connected, bool wait );
	ssize_t recvRem( char *buf, size_t expected, char *prevBuf, size_t prevSize, bool &connected );
	bool done();
	void print( FILE *f = stdout );
};

#endif
