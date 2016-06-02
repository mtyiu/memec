#ifndef __SERVER_SOCKET_CLIENT_SOCKET_HH__
#define __SERVER_SOCKET_CLIENT_SOCKET_HH__

#include "../../common/ds/array_map.hh"
#include "../../common/socket/socket.hh"
#include "../backup/backup.hh"

class ClientSocket : public Socket {
private:
	static ArrayMap<int, ClientSocket> *clients;

public:
	ServerBackup backup;

	static void setArrayMap( ArrayMap<int, ClientSocket> *clients );
	bool start();
	void stop();
};

#endif
