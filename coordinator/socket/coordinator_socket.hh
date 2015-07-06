#ifndef __COORDINATOR_SOCKET_COORDINATOR_SOCKET_HH__
#define __COORDINATOR_SOCKET_COORDINATOR_SOCKET_HH__

#include <vector>
#include "master_socket.hh"
#include "slave_socket.hh"
#include "../../common/ds/array_map.hh"
#include "../../common/socket/socket.hh"
#include "../../common/socket/epoll.hh"

class CoordinatorSocket : public Socket {
public:
	EPoll *epoll;
	ArrayMap<int, struct sockaddr_in> sockets;

	bool init( int type, unsigned long addr, unsigned short port, int numSlaves, EPoll *epoll );
	bool start();
	static bool handler( int fd, uint32_t events, void *data );
};

#endif
