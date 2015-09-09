#include "application_socket.hh"
#include "../../common/util/debug.hh"

ArrayMap<int, ApplicationSocket> *ApplicationSocket::applications;

void ApplicationSocket::setArrayMap( ArrayMap<int, ApplicationSocket> *applications ) {
	ApplicationSocket::applications = applications;
	applications->needsDelete = false;
}

ApplicationSocket::ApplicationSocket() {
	pthread_mutex_init( &this->lock, 0 );
	this->stopped = false;
}

bool ApplicationSocket::start() {
	return this->connect();
}

void ApplicationSocket::stop() {
	// if ( this->sockfd == -1 ) return;
	// pthread_mutex_lock( &this->lock );
	{ // if ( ! this->stopped ) {
		// fprintf( stderr, "Releasing ApplicationSocket: 0x%p\n", this );
		ApplicationSocket::applications->remove( this->sockfd );
		Socket::stop();
		delete this;
		// this->stopped = true;
	}
	// pthread_mutex_unlock( &this->lock );
}

ssize_t ApplicationSocket::send( char *buf, size_t ulen, bool &connected ) {
	return Socket::send( this->sockfd, buf, ulen, connected );
}

ssize_t ApplicationSocket::recv( char *buf, size_t ulen, bool &connected, bool wait ) {
	return Socket::recv( this->sockfd, buf, ulen, connected, wait );
}

ssize_t ApplicationSocket::recvRem( char *buf, size_t expected, char *prevBuf, size_t prevSize, bool &connected ) {
	return Socket::recvRem( this->sockfd, buf, expected, prevBuf, prevSize, connected );
}

bool ApplicationSocket::done() {
	return Socket::done( this->sockfd );
}
