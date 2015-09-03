#ifndef __MASTER_SOCKET_SLAVE_SOCKET_HH__
#define __MASTER_SOCKET_SLAVE_SOCKET_HH__

#include "../../common/ds/array_map.hh"
#include "../../common/socket/socket.hh"

class SlaveSocket : public Socket {
private:
	static ArrayMap<int, SlaveSocket> *slaves;

public:
	bool registered;

	static void setArrayMap( ArrayMap<int, SlaveSocket> *slaves );
	bool start();
	void stop();
	ssize_t send( char *buf, size_t ulen, bool &connected );
	ssize_t recv( char *buf, size_t ulen, bool &connected, bool wait );
	ssize_t recvRem( char *buf, size_t expected, char *prevBuf, size_t prevSize, bool &connected );
	bool done();
	bool ready();
	void print( FILE *f = stdout );
};

#endif
