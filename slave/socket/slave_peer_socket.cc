#include "slave_peer_socket.hh"
#include "../event/slave_peer_event.hh"
#include "../main/slave.hh"
#include "../../common/util/debug.hh"

void SlavePeerSocket::registerTo() {
	Slave *slave = Slave::getInstance();
	SlavePeerEvent event;
	event.reqRegister( this );
	slave->eventQueue.insert( event );
}

SlavePeerSocket::SlavePeerSocket() {
	this->identifier = 0;
	this->registered = false;
	this->self = false;
}

bool SlavePeerSocket::init( ServerAddr &addr, EPoll *epoll, bool active ) {
	this->identifier = strdup( addr.name );
	this->active = active;
	this->epoll = epoll;
	return Socket::init( addr, epoll );
}

bool SlavePeerSocket::start() {
	if ( this->self )
		return true;
	this->registered = false;

	if ( this->active ) {
		if ( this->connect() ) {
			this->registerTo();
			return true;
		}
		return false;
	}
	return true;
}

void SlavePeerSocket::stop() {
	Socket::stop();
}

void SlavePeerSocket::free() {
	if ( this->identifier ) {
		::free( this->identifier );
		this->identifier = 0;
	}
}

bool SlavePeerSocket::isMatched( ServerAddr &serverAddr ) {
	return (
		serverAddr.addr == this->addr.sin_addr.s_addr &&
		serverAddr.port == this->addr.sin_port &&
		strcmp( serverAddr.name, this->identifier ) == 0
	);
}

bool SlavePeerSocket::setRecvFd( int fd, struct sockaddr_in *addr ) {
	this->recvSockFd = fd;
	this->recvAddr = *addr;

	if ( fd != this->sockfd ) {
		this->epoll->remove( fd );
		dup2( fd, this->sockfd );
		this->epoll->add( this->sockfd, EPOLL_EVENT_SET );
		return true;
	} else {
		if ( ! this->registered )
			this->registerTo();
		return false;
	}
}

ssize_t SlavePeerSocket::send( char *buf, size_t ulen, bool &connected ) {
	if ( this->self ) {
		__ERROR__( "SlavePeerSocket", "send", "send() should not be called for self-socket!" );
		return 0;
	}
	return Socket::send( this->sockfd, buf, ulen, connected );
}

ssize_t SlavePeerSocket::recv( char *buf, size_t ulen, bool &connected, bool wait ) {
	if ( this->self ) {
		__ERROR__( "SlavePeerSocket", "recv", "recv() should not be called for self-socket!" );
		return 0;
	}
	return Socket::recv( this->sockfd, buf, ulen, connected, wait );
}

void SlavePeerSocket::print( FILE *f ) {
	char buf[ 16 ];
	Socket::ntoh_ip( this->addr.sin_addr.s_addr, buf, 16 );
	fprintf( f, "[%4d] %s:%u ", this->sockfd, buf, Socket::ntoh_port( this->addr.sin_port ) );
	if ( this->self )
		fprintf( f, "(self)\n" );
	else if ( this->connected )
		fprintf( f, "(connected %s registered, %s)\n", this->registered ? "and" : "but not", this->recvSockFd != -1 ? "bidirectional" : "unidirectional" );
	else
		fprintf( f, "(disconnected)\n" );
}
