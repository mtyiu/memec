#include <cerrno>
#include "server_peer_socket.hh"
#include "../event/server_peer_event.hh"
#include "../main/server.hh"
#include "../../common/util/debug.hh"

ArrayMap<int, ServerPeerSocket> *ServerPeerSocket::slavePeers;

void ServerPeerSocket::setArrayMap( ArrayMap<int, ServerPeerSocket> *slavePeers ) {
	ServerPeerSocket::slavePeers = slavePeers;
	slavePeers->needsDelete = false;
}

void ServerPeerSocket::registerTo() {
	Slave *slave = Slave::getInstance();
	SlavePeerEvent event;
	event.reqRegister( this );
	slave->eventQueue.insert( event );
}

ServerPeerSocket::ServerPeerSocket() {
	this->identifier = 0;
	this->received = false;
	this->registered = false;
	this->self = false;
}

bool ServerPeerSocket::init( int tmpfd, ServerAddr &addr, EPoll *epoll, bool self ) {
	this->identifier = strdup( addr.name );
	this->self = self;
	this->epoll = epoll;

	this->mode = SOCKET_MODE_UNDEFINED;
	this->connected = self;
	this->sockfd = tmpfd;
	memset( &this->addr, 0, sizeof( this->addr ) );
	this->type = addr.type;
	this->addr.sin_family = AF_INET;
	this->addr.sin_port = addr.port;
	this->addr.sin_addr.s_addr = addr.addr;
	return true;
}

int ServerPeerSocket::init() {
	if ( this->self )
		return 0;
	this->registered = false;

	this->sockfd = socket( AF_INET, this->type, 0 );
	if ( this->sockfd < 0 ) {
		__ERROR__( "ServerPeerSocket", "start", "%s", strerror( errno ) );
		return -1;
	}

	this->setReuse();
	this->setNoDelay();

	return this->sockfd;
}

bool ServerPeerSocket::start() {
	if ( this->connect() ) {
		this->received = true;
		this->epoll->add( this->sockfd, EPOLL_EVENT_SET );
		this->registerTo();
		return true;
	}
	return false;
}

void ServerPeerSocket::stop() {
	if ( ! this->self ) {
		int newFd = -INT_MIN + this->sockfd;
		ServerPeerSocket::slavePeers->replaceKey( this->sockfd, newFd );
		this->sockfd = newFd;
	}
	Socket::stop();
}

bool ServerPeerSocket::ready() {
	return this->self || ( this->connected && this->registered );
}

void ServerPeerSocket::free() {
	if ( this->identifier ) {
		::free( this->identifier );
		this->identifier = 0;
	}
}

bool ServerPeerSocket::setRecvFd( int fd, struct sockaddr_in *addr ) {
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

ssize_t ServerPeerSocket::send( char *buf, size_t ulen, bool &connected ) {
	if ( this->self ) {
		__ERROR__( "ServerPeerSocket", "send", "send() should not be called for self-socket (opcode = 0x%x)!", buf[ 1 ] );
		return 0;
	}
	return Socket::send( this->sockfd, buf, ulen, connected );
}

ssize_t ServerPeerSocket::recv( char *buf, size_t ulen, bool &connected, bool wait ) {
	if ( this->self ) {
		__ERROR__( "ServerPeerSocket", "recv", "recv() should not be called for self-socket!" );
		return 0;
	}
	return Socket::recv( this->sockfd, buf, ulen, connected, wait );
}

ssize_t ServerPeerSocket::recvRem( char *buf, size_t expected, char *prevBuf, size_t prevSize, bool &connected ) {
	return Socket::recvRem( this->sockfd, buf, expected, prevBuf, prevSize, connected );
}

bool ServerPeerSocket::done() {
	return Socket::done( this->sockfd );
}

void ServerPeerSocket::print( FILE *f ) {
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
