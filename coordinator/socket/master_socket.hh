#ifndef __COORDINATOR_SOCKET_MASTER_SOCKET_HH__
#define __COORDINATOR_SOCKET_MASTER_SOCKET_HH__

#include "../../common/socket/socket.hh"

class MasterSocket : public Socket {
public:
	struct {
		uint32_t addr;
		uint16_t port;
	} listenAddr;

	bool start();
	void setListenAddr( uint32_t addr, uint16_t port );
	ssize_t send( char *buf, size_t ulen, bool &connected );
	ssize_t recv( char *buf, size_t ulen, bool &connected, bool wait );
};

#endif
