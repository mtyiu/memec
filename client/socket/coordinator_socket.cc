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

void CoordinatorSocket::print( FILE *f ) {
	char buf[ 16 ];
	Socket::ntoh_ip( this->addr.sin_addr.s_addr, buf, 16 );
	fprintf( f, "[%4d] %s:%u ", this->sockfd, buf, Socket::ntoh_port( this->addr.sin_port ) );
	if ( this->connected )
		fprintf( f, "(connected %s registered)\n", this->registered ? "and" : "but not" );
	else
		fprintf( f, "(disconnected)\n" );
}
