#include "../event/coordinator_event.hh"
#include "../main/client.hh"
#include "coordinator_socket.hh"

ArrayMap<int, CoordinatorSocket> *CoordinatorSocket::coordinators;

void CoordinatorSocket::setArrayMap( ArrayMap<int, CoordinatorSocket> *coordinators ) {
	CoordinatorSocket::coordinators = coordinators;
	coordinators->needsDelete = false;
}

bool CoordinatorSocket::start() {
	this->registered = false;
	if ( this->connect() ) {
		Client *client = Client::getInstance();
		CoordinatorEvent event;
		event.reqRegister( this, client->config.client.client.addr.addr, client->config.client.client.addr.port );
		client->eventQueue.insert( event );
		return true;
	}
	return false;
}

void CoordinatorSocket::stop() {
	CoordinatorSocket::coordinators->remove( this->sockfd );
	Socket::stop();
}

ssize_t CoordinatorSocket::send( char *buf, size_t ulen, bool &connected ) {
	return Socket::send( this->sockfd, buf, ulen, connected );
}
ssize_t CoordinatorSocket::recv( char *buf, size_t ulen, bool &connected, bool wait ) {
	return Socket::recv( this->sockfd, buf, ulen, connected, wait );
}

ssize_t CoordinatorSocket::recvRem( char *buf, size_t expected, char *prevBuf, size_t prevSize, bool &connected ) {
	return Socket::recvRem( this->sockfd, buf, expected, prevBuf, prevSize, connected );
}

bool CoordinatorSocket::done() {
	return Socket::done( this->sockfd );
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
