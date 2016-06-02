#include "../event/client_event.hh"
#include "../main/application.hh"
#include "client_socket.hh"

ArrayMap<int, ClientSocket> *ClientSocket::clients;

void ClientSocket::setArrayMap( ArrayMap<int, ClientSocket> *clients ) {
	ClientSocket::clients = clients;
	clients->needsDelete = false;
}

bool ClientSocket::start() {
	this->registered = false;
	if ( this->connect() ) {
		Application *application = Application::getInstance();
		ClientEvent event;
		event.reqRegister( this );
		application->eventQueue.insert( event );
		return true;
	}
	return false;
}

void ClientSocket::print( FILE *f ) {
	char buf[ 16 ];
	Socket::ntoh_ip( this->addr.sin_addr.s_addr, buf, 16 );
	fprintf( f, "[%4d] %s:%u ", this->sockfd, buf, Socket::ntoh_port( this->addr.sin_port ) );
	if ( this->connected )
		fprintf( f, "(connected %s registered)\n", this->registered ? "and" : "but not" );
	else
		fprintf( f, "(disconnected)\n" );
}
