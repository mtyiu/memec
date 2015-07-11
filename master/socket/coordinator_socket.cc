#include "../event/coordinator_event.hh"
#include "../main/master.hh"
#include "coordinator_socket.hh"

bool CoordinatorSocket::start() {
	this->registered = false;
	if ( this->connect() ) {
		Master *master = Master::getInstance();
		CoordinatorEvent event;
		event.reqRegister( this );
		master->eventQueue.insert( event );
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
	fprintf( f, "[%4d] %s:%u ", this->sockfd, buf, Socket::ntoh_port( this->addr.sin_port ) );
	if ( this->connected )
		fprintf( f, "(connected %s registered)\n", this->registered ? "and" : "but not" );
	else
		fprintf( f, "(disconnected)\n" );
}
