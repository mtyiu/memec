#include "../event/master_event.hh"
#include "../main/application.hh"
#include "master_socket.hh"

ArrayMap<int, MasterSocket> *MasterSocket::masters;

void MasterSocket::setArrayMap( ArrayMap<int, MasterSocket> *masters ) {
	MasterSocket::masters = masters;
	masters->needsDelete = false;
}

bool MasterSocket::start() {
	this->registered = false;
	if ( this->connect() ) {
		Application *application = Application::getInstance();
		MasterEvent event;
		event.reqRegister( this );
		application->eventQueue.insert( event );
		return true;
	}
	return false;
}

void MasterSocket::stop() {
	// MasterSocket::masters->remove( this->sockfd );
	Socket::stop();
	// delete this;
}

ssize_t MasterSocket::send( char *buf, size_t ulen, bool &connected ) {
	return Socket::send( this->sockfd, buf, ulen, connected );
}

ssize_t MasterSocket::recv( char *buf, size_t ulen, bool &connected, bool wait ) {
	return Socket::recv( this->sockfd, buf, ulen, connected, wait );
}

ssize_t MasterSocket::recvRem( char *buf, size_t expected, char *prevBuf, size_t prevSize, bool &connected ) {
	return Socket::recvRem( this->sockfd, buf, expected, prevBuf, prevSize, connected );
}

bool MasterSocket::done() {
	return Socket::done( this->sockfd );
}

void MasterSocket::print( FILE *f ) {
	char buf[ 16 ];
	Socket::ntoh_ip( this->addr.sin_addr.s_addr, buf, 16 );
	fprintf( f, "[%4d] %s:%u ", this->sockfd, buf, Socket::ntoh_port( this->addr.sin_port ) );
	if ( this->connected )
		fprintf( f, "(connected %s registered)\n", this->registered ? "and" : "but not" );
	else
		fprintf( f, "(disconnected)\n" );
}
