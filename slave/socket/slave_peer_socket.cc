#include <cerrno>
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
	this->received = false;
	this->registered = false;
	this->self = false;
}

bool SlavePeerSocket::init( ServerAddr &addr, EPoll *epoll, bool self ) {
	this->identifier = strdup( addr.name );
	this->self = self;
	this->epoll = epoll;

	this->mode = SOCKET_MODE_UNDEFINED;
	this->connected = self;
	this->sockfd = -1;
	memset( &this->addr, 0, sizeof( this->addr ) );
	this->type = addr.type;
	this->addr.sin_family = AF_INET;
	this->addr.sin_port = addr.port;
	this->addr.sin_addr.s_addr = addr.addr;
	return true;
}

bool SlavePeerSocket::start() {
	if ( this->self )
		return true;
	this->registered = false;

	this->sockfd = socket( AF_INET, this->type, 0 );
	if ( this->sockfd < 0 ) {
		__ERROR__( "SlavePeerSocket", "start", "%s", strerror( errno ) );
		return false;
	}

	if ( ! this->setNonBlocking() )
		return false;

	this->setReuse();
	this->setNoDelay();

	this->epoll->add( this->sockfd, EPOLL_EVENT_SET );

	if ( this->connect() ) {
		this->registerTo();
		return true;
	}
	return false;
}

void SlavePeerSocket::stop() {
	printf( "fd = %d is stopping...\n", this->sockfd );
	if ( this->sockfd != -1 )
		Socket::stop();
}

bool SlavePeerSocket::ready() {
	return this->self || ( Socket::ready() );
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
	bool ret = false;
	this->received = true;
	this->recvAddr = *addr;

	if ( fd != this->sockfd ) {
		this->sockfd = fd;
		ret = true;
	}
	if ( ! this->registered )
		this->registerTo();
	return ret;
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
		fprintf( f,
			"(connected %s registered, %s)\n",
			this->registered ? "and" : "but not",
			this->received ? "bidirectional" : "unidirectional"
		);
	else
		fprintf( f, "(disconnected)\n" );
}
