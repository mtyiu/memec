#ifndef __APPLICATION_SOCKET_MASTER_SOCKET_HH__
#define __APPLICATION_SOCKET_MASTER_SOCKET_HH__

#include "../../common/socket/socket.hh"

class MasterSocket : public Socket {
public:
	bool registered;

	bool start();
	ssize_t send( char *buf, size_t ulen, bool &connected );
	ssize_t recv( char *buf, size_t ulen, bool &connected, bool wait );
	ssize_t recvRem( char *buf, size_t expected, char *prevBuf, size_t prevSize, bool &connected );
	bool done();
	void print( FILE *f = stdout );
};

#endif
