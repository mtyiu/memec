#ifndef __MASTER_SOCKET_HH__
#define __MASTER_SOCKET_HH__

#include "../../common/socket/socket.hh"

class MasterSocket : public Socket {
public:
	bool init( int sockfd, struct sockaddr_in addr );
	bool start();
	ssize_t send( char *buf, size_t ulen, bool &connected );
	ssize_t recv( char *buf, size_t ulen, bool &connected, bool wait );
};

#endif
