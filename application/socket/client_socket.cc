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

ssize_t ClientSocket::send( char *buf, size_t ulen, bool &connected ) {
	return Socket::send( this->sockfd, buf, ulen, connected );
}

ssize_t ClientSocket::recv( char *buf, size_t ulen, bool &connected, bool wait ) {
	return Socket::recv( this->sockfd, buf, ulen, connected, wait );
}

ssize_t ClientSocket::recvRem( char *buf, size_t expected, char *prevBuf, size_t prevSize, bool &connected ) {
	return Socket::recvRem( this->sockfd, buf, expected, prevBuf, prevSize, connected );
}

bool ClientSocket::done() {
	return Socket::done( this->sockfd );
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
