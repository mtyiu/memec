#include "../event/slave_peer_event.hh"
#include "../main/slave.hh"
#include "slave_peer_socket.hh"

bool SlavePeerSocket::start() {
	if ( this->connect() ) {
		Slave *slave = Slave::getInstance();
		SlavePeerEvent event;
		event.reqRegister( this );
		slave->eventQueue.insert( event );
		return true;
	}
	return false;
}

ssize_t SlavePeerSocket::send( char *buf, size_t ulen, bool &connected ) {
	return Socket::send( this->sockfd, buf, ulen, connected );
}
ssize_t SlavePeerSocket::recv( char *buf, size_t ulen, bool &connected, bool wait ) {
	return Socket::recv( this->sockfd, buf, ulen, connected, wait );
}
