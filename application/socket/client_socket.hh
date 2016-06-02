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
	void print( FILE *f = stdout );
};

#endif
