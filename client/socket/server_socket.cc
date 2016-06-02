#include "../event/server_event.hh"
#include "../main/client.hh"
#include "server_socket.hh"

ArrayMap<int, ServerSocket> *ServerSocket::servers;

void ServerSocket::setArrayMap( ArrayMap<int, ServerSocket> *servers ) {
	ServerSocket::servers = servers;
	servers->needsDelete = false;
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

void ServerSocket::registerClient() {
	Client *client = Client::getInstance();
	ServerEvent event;
	event.reqRegister( this, client->config.client.client.addr.addr, client->config.client.client.addr.port );
	client->eventQueue.insert( event );
}

void ServerSocket::stop() {
	int newFd = -INT_MIN + this->sockfd;
	ServerSocket::servers->replaceKey( this->sockfd, newFd );
	this->sockfd = newFd;
	Socket::stop();
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
