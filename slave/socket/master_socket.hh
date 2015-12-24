#ifndef __SLAVE_SOCKET_MASTER_SOCKET_HH__
#define __SLAVE_SOCKET_MASTER_SOCKET_HH__

#include "../../common/ds/array_map.hh"
#include "../../common/socket/socket.hh"
#include "../backup/backup.hh"

class MasterSocket : public Socket {
private:
	static ArrayMap<int, MasterSocket> *masters;

public:
	SlaveBackup backup;

	static void setArrayMap( ArrayMap<int, MasterSocket> *masters );
	bool start();
	void stop();
	ssize_t send( char *buf, size_t ulen, bool &connected );
	ssize_t recv( char *buf, size_t ulen, bool &connected, bool wait );
	ssize_t recvRem( char *buf, size_t expected, char *prevBuf, size_t prevSize, bool &connected );
	bool done();
};

#endif
