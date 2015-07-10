#include "../event/coordinator_event.hh"
#include "../main/slave.hh"
#include "coordinator_socket.hh"

bool CoordinatorSocket::start() {
	if ( this->connect() ) {
		Slave *slave = Slave::getInstance();
		CoordinatorEvent event;
		event.reqRegister( this );
		slave->eventQueue.insert( event );
		return true;
	}
	return false;
}

ssize_t CoordinatorSocket::send( char *buf, size_t ulen, bool &connected ) {
	return Socket::send( this->sockfd, buf, ulen, connected );
}
ssize_t CoordinatorSocket::recv( char *buf, size_t ulen, bool &connected, bool wait ) {
	return Socket::recv( this->sockfd, buf, ulen, connected, wait );
}
