#ifndef __APPLICATION_SOCKET_CLIENT_SOCKET_HH__
#define __APPLICATION_SOCKET_CLIENT_SOCKET_HH__

#include "../../common/ds/array_map.hh"
#include "../../common/socket/socket.hh"

class ClientSocket : public Socket {
private:
	static ArrayMap<int, ClientSocket> *clients;

public:
	bool registered;

	static void setArrayMap( ArrayMap<int, ClientSocket> *clients );
	bool start();
	ssize_t send( char *buf, size_t ulen, bool &connected );
	ssize_t recv( char *buf, size_t ulen, bool &connected, bool wait );
	ssize_t recvRem( char *buf, size_t expected, char *prevBuf, size_t prevSize, bool &connected );
	bool done();
	void print( FILE *f = stdout );
};

#endif
