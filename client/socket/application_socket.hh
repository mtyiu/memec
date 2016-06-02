#ifndef __CLIENT_SOCKET_APPLICATION_SOCKET_HH__
#define __CLIENT_SOCKET_APPLICATION_SOCKET_HH__

#include "../../common/ds/array_map.hh"
#include "../../common/socket/socket.hh"

class ApplicationSocket : public Socket {
private:
	static ArrayMap<int, ApplicationSocket> *applications;

public:
	static void setArrayMap( ArrayMap<int, ApplicationSocket> *applications );
	bool start();
	void stop();
};

#endif
