#ifndef __MASTER_SOCKET_COORDINATOR_SOCKET_HH__
#define __MASTER_SOCKET_COORDINATOR_SOCKET_HH__

#include "../../common/socket/socket.hh"

class CoordinatorSocket : public Socket {
public:
	bool start();
	ssize_t send( char *buf, size_t ulen, bool &connected );
	ssize_t recv( char *buf, size_t ulen, bool &connected, bool wait );
};

#endif
