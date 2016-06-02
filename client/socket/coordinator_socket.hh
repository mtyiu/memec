#ifndef __CLIENT_SOCKET_COORDINATOR_SOCKET_HH__
#define __CLIENT_SOCKET_COORDINATOR_SOCKET_HH__

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
	void print( FILE *f = stdout );
};

#endif
