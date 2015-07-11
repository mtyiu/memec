#include "../event/slave_peer_event.hh"
#include "../main/slave.hh"
#include "slave_peer_socket.hh"

bool SlavePeerSocket::start() {
	this->registered = false;
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

void SlavePeerSocket::print( FILE *f ) {
	char buf[ 16 ];
	Socket::ntoh_ip( this->addr.sin_addr.s_addr, buf, 16 );
	fprintf( f, "[%4d] %s:%u ", this->sockfd, buf, Socket::ntoh_port( this->addr.sin_port ) );
	if ( this->connected )
		fprintf( f, "(connected %s registered)\n", this->registered ? "and" : "but not" );
	else
		fprintf( f, "(disconnected)\n" );
}
