#ifndef __SERVER_SOCKET_CLIENT_SOCKET_HH__
#define __SERVER_SOCKET_CLIENT_SOCKET_HH__

#include "../../common/ds/array_map.hh"
#include "../../common/socket/socket.hh"
#include "../backup/backup.hh"

class ClientSocket : public Socket {
private:
	static ArrayMap<int, ClientSocket> *masters;

public:
	ServerBackup backup;

	static void setArrayMap( ArrayMap<int, ClientSocket> *masters );
	bool start();
	void stop();
	ssize_t send( char *buf, size_t ulen, bool &connected );
	ssize_t recv( char *buf, size_t ulen, bool &connected, bool wait );
	ssize_t recvRem( char *buf, size_t expected, char *prevBuf, size_t prevSize, bool &connected );
	bool done();
};

#endif
