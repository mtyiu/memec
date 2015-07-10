#ifndef __SLAVE_SOCKET_SLAVE_PEER_SOCKET_HH__
#define __SLAVE_SOCKET_SLAVE_PEER_SOCKET_HH__

#include "../../common/socket/socket.hh"

class SlavePeerSocket : public Socket {
public:
	bool start();
	ssize_t send( char *buf, size_t ulen, bool &connected );
	ssize_t recv( char *buf, size_t ulen, bool &connected, bool wait );
};

#endif
