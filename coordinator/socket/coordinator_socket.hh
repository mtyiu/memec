#ifndef __COORDINATOR_SOCKET_HH__
#define __COORDINATOR_SOCKET_HH__

#include "../../common/socket/socket.hh"
#include "../../common/socket/epoll.hh"

class CoordinatorSocket : public Socket {
public:
	EPoll epoll;

	bool prepare( int maxEvents, int timeout );
	bool start();
	static bool handler( int fd, uint32_t events, void *data );
};

#endif
