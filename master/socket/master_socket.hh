#ifndef __MASTER_SOCKET_MASTER_SOCKET_HH__
#define __MASTER_SOCKET_MASTER_SOCKET_HH__

#include <vector>
#include <pthread.h>
#include "../protocol/protocol.hh"
#include "../../common/ds/array_map.hh"
#include "../../common/socket/socket.hh"
#include "../../common/socket/epoll.hh"

class MasterSocket : public Socket {
public:
	bool isRunning;
	pthread_t tid;
	EPoll *epoll;
	ArrayMap<int, struct sockaddr_in> sockets;
	MasterProtocol protocol;
	struct {
		size_t size;
		char data[ PROTO_HEADER_SIZE ];
	} buffer;

	MasterSocket();
	bool init( int type, unsigned long addr, unsigned short port, EPoll *epoll );
	bool start();
	void stop();
	void debug();

	static void *run( void *argv );
	static bool handler( int fd, uint32_t events, void *data );
};

#endif
