#include "server_socket.hh"
#include "../main/coordinator.hh"
#include "../event/server_event.hh"

ArrayMap<int, ServerSocket> *ServerSocket::servers;

void ServerSocket::setArrayMap( ArrayMap<int, ServerSocket> *servers ) {
	ServerSocket::servers = servers;
	servers->needsDelete = false;
}

bool ServerSocket::init( int tmpfd, ServerAddr &addr, EPoll *epoll ) {
	this->identifier = strdup( addr.name );
	this->mode = SOCKET_MODE_UNDEFINED;
	this->connected = false;
	this->sockfd = tmpfd;
	memset( &this->addr, 0, sizeof( this->addr ) );
	this->type = addr.type;
	this->addr.sin_family = AF_INET;
	this->addr.sin_port = addr.port;
	this->addr.sin_addr.s_addr = addr.addr;
	return true;
}

bool ServerSocket::start() {
	return this->connect();
}

void ServerSocket::stop() {
	int newFd = - this->sockfd;

	ServerSocket::servers->replaceKey( this->sockfd, newFd );
	Socket::stop();

	ServerEvent event;
	event.disconnect( this );
	Coordinator::getInstance()->eventQueue.insert( event );

	// TODO: Fix memory leakage!
	// delete this;
}

bool ServerSocket::setRecvFd( int fd, struct sockaddr_in *addr ) {
	bool ret = false;
	this->recvAddr = *addr;
	this->mode = SOCKET_MODE_CONNECT;
	this->connected = true;

	if ( fd != this->sockfd ) {
		this->sockfd = fd;
		ret = true;
	}
	return ret;
}
