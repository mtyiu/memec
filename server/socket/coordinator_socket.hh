#ifndef __SERVER_SOCKET_COORDINATOR_SOCKET_HH__
#define __SERVER_SOCKET_COORDINATOR_SOCKET_HH__

#include "../../common/ds/array_map.hh"
#include "../../common/socket/socket.hh"

class CoordinatorSocket : public Socket {
private:
	static ArrayMap<int, CoordinatorSocket> *coordinators;

public:
	bool registered;

	static void setArrayMap( ArrayMap<int, CoordinatorSocket> *coordinators );
	bool start();
	void stop();
	ssize_t send( char *buf, size_t ulen, bool &connected );
	ssize_t recv( char *buf, size_t ulen, bool &connected, bool wait );
	ssize_t recvRem( char *buf, size_t expected, char *prevBuf, size_t prevSize, bool &connected );
	bool done();
	void print( FILE *f = stdout );
};

#endif
