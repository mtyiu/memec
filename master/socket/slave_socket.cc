#include "../event/slave_event.hh"
#include "../main/master.hh"
#include "slave_socket.hh"

bool SlaveSocket::start() {
	this->registered = false;
	if ( this->connect() ) {
		Master *master = Master::getInstance();
		SlaveEvent event;
		event.reqRegister( this, master->config.master.master.addr.addr, master->config.master.master.addr.port );
		master->eventQueue.insert( event );
		return true;
	}
	return false;
}

ssize_t SlaveSocket::send( char *buf, size_t ulen, bool &connected ) {
	ssize_t bytes = Socket::send( this->sockfd, buf, ulen, connected );
	__DEBUG__( MAGENTA, "SlaveSocket", "send", "Sent %ld bytes...", bytes );
	return bytes;
}

ssize_t SlaveSocket::recv( char *buf, size_t ulen, bool &connected, bool wait ) {
	ssize_t bytes = Socket::recv( this->sockfd, buf, ulen, connected, wait );
	__DEBUG__( MAGENTA, "SlaveSocket", "recv", "Received %ld bytes...", bytes );
	return bytes;
}

bool SlaveSocket::ready() {
	return this->connected && this->registered;
}

void SlaveSocket::print( FILE *f ) {
	char buf[ 16 ];
	Socket::ntoh_ip( this->addr.sin_addr.s_addr, buf, 16 );
	fprintf( f, "[%4d] %s:%u ", this->sockfd, buf, Socket::ntoh_port( this->addr.sin_port ) );
	if ( this->connected )
		fprintf( f, "(connected %s registered)\n", this->registered ? "and" : "but not" );
	else
		fprintf( f, "(disconnected)\n" );
}
