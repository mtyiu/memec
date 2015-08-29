#ifndef __MASTER_SOCKET_APPLICATION_SOCKET_HH__
#define __MASTER_SOCKET_APPLICATION_SOCKET_HH__

#include "../../common/socket/socket.hh"

class ApplicationSocket : public Socket {
public:
	bool start();
	ssize_t send( char *buf, size_t ulen, bool &connected );
	ssize_t recv( char *buf, size_t ulen, bool &connected, bool wait );
	ssize_t recvRem( char *buf, size_t expected, char *prevBuf, size_t prevSize, bool &connected );
	bool done();
};

#endif
