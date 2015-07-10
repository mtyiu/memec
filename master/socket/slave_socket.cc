#include "../event/slave_event.hh"
#include "../main/master.hh"
#include "slave_socket.hh"

bool SlaveSocket::start() {
	if ( this->connect() ) {
		Master *master = Master::getInstance();
		SlaveEvent event;
		event.reqRegister( this );
		master->eventQueue.insert( event );
		return true;
	}
	return false;
}

ssize_t SlaveSocket::send( char *buf, size_t ulen, bool &connected ) {
	return Socket::send( this->sockfd, buf, ulen, connected );
}
ssize_t SlaveSocket::recv( char *buf, size_t ulen, bool &connected, bool wait ) {
	return Socket::recv( this->sockfd, buf, ulen, connected, wait );
}
