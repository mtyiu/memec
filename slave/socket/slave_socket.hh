#ifndef __MASTER_SOCKET_HH__
#define __MASTER_SOCKET_HH__

#include <vector>
#include "../../common/ds/array_map.hh"
#include "../../common/socket/socket.hh"
#include "../../common/socket/epoll.hh"

class SlaveSocket : public Socket {
public:
	EPoll epoll;
	ArrayMap<int, struct sockaddr_in> temps;

	bool init( int type, unsigned long addr, unsigned short port, int maxEvents, int timeout );
	bool start();
	static bool handler( int fd, uint32_t events, void *data );
};

#endif
