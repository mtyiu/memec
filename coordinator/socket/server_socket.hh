#ifndef __COORDINATOR_SOCKET_SERVER_SOCKET_HH__
#define __COORDINATOR_SOCKET_SERVER_SOCKET_HH__

#include <unordered_map>
#include "../ds/map.hh"
#include "../../common/ds/array_map.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/lock/lock.hh"
#include "../../common/socket/socket.hh"

class ServerSocket : public Socket {
private:
	static ArrayMap<int, ServerSocket> *servers;
	struct sockaddr_in recvAddr;
	char *identifier;

public:
	uint16_t instanceId;
	Map map;
	ServerSocket *failed;

	static void setArrayMap( ArrayMap<int, ServerSocket> *servers );
	bool init( int tmpfd, ServerAddr &addr, EPoll *epoll );
	bool start();
	void stop();
	bool setRecvFd( int fd, struct sockaddr_in *addr );
};

#endif
