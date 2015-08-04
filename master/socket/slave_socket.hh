#ifndef __MASTER_SOCKET_SLAVE_SOCKET_HH__
#define __MASTER_SOCKET_SLAVE_SOCKET_HH__

#include "../../common/socket/socket.hh"

class SlaveSocket : public Socket {
public:
	bool registered;
	
	bool start();
	ssize_t send( char *buf, size_t ulen, bool &connected );
	ssize_t recv( char *buf, size_t ulen, bool &connected, bool wait );
	bool ready();
	void print( FILE *f = stdout );
};

#endif
