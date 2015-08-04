#ifndef __COORDINATOR_SOCKET_SLAVE_SOCKET_HH__
#define __COORDINATOR_SOCKET_SLAVE_SOCKET_HH__

#include <map>
#include "../../common/ds/key.hh"
#include "../../common/ds/load.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/socket/socket.hh"

class SlaveSocket : public Socket {
public:
	Load load;
	// All stored keys
	std::map<Key, Metadata> keys;

	bool start();
	ssize_t send( char *buf, size_t ulen, bool &connected );
	ssize_t recv( char *buf, size_t ulen, bool &connected, bool wait );
};

#endif
