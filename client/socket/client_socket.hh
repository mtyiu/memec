#ifndef __CLIENT_SOCKET_CLIENT_SOCKET_HH__
#define __CLIENT_SOCKET_CLIENT_SOCKET_HH__

#include <vector>
#include <pthread.h>
#include "../protocol/protocol.hh"
#include "../../common/ds/array_map.hh"
#include "../../common/socket/socket.hh"
#include "../../common/socket/epoll.hh"

class ClientSocket : public Socket {
public:
	bool isRunning;
	pthread_t tid;
	EPoll *epoll;
	ArrayMap<int, struct sockaddr_in> sockets;
	ClientProtocol protocol;
	struct {
		size_t size;
		char data[ PROTO_HEADER_SIZE ];
	} buffer;

	ClientSocket();
	bool init( int type, uint32_t addr, uint16_t port, EPoll *epoll );
	bool start();
	void stop();
	void print( FILE *f = stdout );
	void printThread( FILE *f = stdout );

	static void *run( void *argv );
	static bool handler( int fd, uint32_t events, void *data );
};

#endif
