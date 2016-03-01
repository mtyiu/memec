#include "../event/server_event.hh"
#include "../main/client.hh"
#include "server_socket.hh"

ArrayMap<int, ServerSocket> *ServerSocket::slaves;

void ServerSocket::setArrayMap( ArrayMap<int, ServerSocket> *slaves ) {
	ServerSocket::slaves = slaves;
	slaves->needsDelete = false;
}

bool ServerSocket::start() {
	LOCK_INIT( &this->timestamp.pendingAck.updateLock );
	LOCK_INIT( &this->timestamp.pendingAck.delLock );
	LOCK_INIT( &this->ackParityDeltaBackupLock );

	this->registered = false;
	if ( this->connect() ) {
		return true;
	}
	return false;
}

void ServerSocket::registerMaster() {
	Master *master = Master::getInstance();
	ServerEvent event;
	event.reqRegister( this, master->config.master.master.addr.addr, master->config.master.master.addr.port );
	master->eventQueue.insert( event );
}

void ServerSocket::stop() {
	int newFd = -INT_MIN + this->sockfd;
	ServerSocket::slaves->replaceKey( this->sockfd, newFd );
	this->sockfd = newFd;
	Socket::stop();
}

ssize_t ServerSocket::send( char *buf, size_t ulen, bool &connected ) {
	return Socket::send( this->sockfd, buf, ulen, connected );
}

ssize_t ServerSocket::recv( char *buf, size_t ulen, bool &connected, bool wait ) {
	return Socket::recv( this->sockfd, buf, ulen, connected, wait );
}

ssize_t ServerSocket::recvRem( char *buf, size_t expected, char *prevBuf, size_t prevSize, bool &connected ) {
	return Socket::recvRem( this->sockfd, buf, expected, prevBuf, prevSize, connected );
}

bool ServerSocket::done() {
	return Socket::done( this->sockfd );
}

bool ServerSocket::ready() {
	return this->connected && this->registered;
}

void ServerSocket::print( FILE *f ) {
	char buf[ 16 ];
	Socket::ntoh_ip( this->addr.sin_addr.s_addr, buf, 16 );
	fprintf( f, "[%4d] %s:%u ", this->sockfd, buf, Socket::ntoh_port( this->addr.sin_port ) );
	if ( this->connected )
		fprintf( f, "(connected %s registered)\n", this->registered ? "and" : "but not" );
	else
		fprintf( f, "(disconnected)\n" );
}
