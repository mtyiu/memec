#include "../event/coordinator_event.hh"
#include "../main/slave.hh"
#include "coordinator_socket.hh"

bool CoordinatorSocket::start() {
	this->registered = false;
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

void CoordinatorSocket::print( FILE *f ) {
	char buf[ 16 ];
	Socket::ntoh_ip( this->addr.sin_addr.s_addr, buf, 16 );
	fprintf( f, "[%4d] %s:%u (%sconnected / %sregistered)\n", this->sockfd, buf, Socket::ntoh_port( this->addr.sin_port ), this->connected ? "" : "not ", this->registered ? "" : "not " );
}
