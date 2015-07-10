#ifndef __SLAVE_SOCKET_SLAVE_PEER_SOCKET_HH__
#define __SLAVE_SOCKET_SLAVE_PEER_SOCKET_HH__

#include "../../common/socket/socket.hh"

class SlavePeerSocket : public Socket {
public:
	bool registered;
	
	bool start();
	ssize_t send( char *buf, size_t ulen, bool &connected );
	ssize_t recv( char *buf, size_t ulen, bool &connected, bool wait );
	void print( FILE *f = stdout );
};

#endif
