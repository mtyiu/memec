#ifndef __COORDINATOR_SOCKET_SLAVE_SOCKET_HH__
#define __COORDINATOR_SOCKET_SLAVE_SOCKET_HH__

#include <map>
#include "../../common/ds/array_map.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/load.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/socket/socket.hh"

class SlaveSocket : public Socket {
private:
	static ArrayMap<int, SlaveSocket> *slaves;

public:
	// All stored keys
	std::map<Key, OpMetadata> keys;
	struct {
		uint32_t addr;
		uint16_t port;
	} listenAddr;

	static void setArrayMap( ArrayMap<int, SlaveSocket> *slaves );
	bool start();
	void stop();
	void setListenAddr( uint32_t addr, uint16_t port );
	ssize_t send( char *buf, size_t ulen, bool &connected );
	ssize_t recv( char *buf, size_t ulen, bool &connected, bool wait );
	ssize_t recvRem( char *buf, size_t expected, char *prevBuf, size_t prevSize, bool &connected );
	bool done();
};

#endif
