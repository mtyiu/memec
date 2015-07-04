#include "coordinator_socket.hh"

bool CoordinatorSocket::start() {
	bool ret = this->listen();
	return ret;
}
