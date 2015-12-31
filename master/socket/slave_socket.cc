#include "../event/slave_event.hh"
#include "../main/master.hh"
#include "slave_socket.hh"

ArrayMap<int, SlaveSocket> *SlaveSocket::slaves;

void SlaveSocket::setArrayMap( ArrayMap<int, SlaveSocket> *slaves ) {
	SlaveSocket::slaves = slaves;
	slaves->needsDelete = false;
}

bool SlaveSocket::start() {
	this->registered = false;
	if ( this->connect() ) {
		return true;
	}
	return false;
}

void SlaveSocket::registerMaster() {
	Master *master = Master::getInstance();
	SlaveEvent event;
	event.reqRegister( this, master->config.master.master.addr.addr, master->config.master.master.addr.port );
	master->eventQueue.insert( event );
}

void SlaveSocket::stop() {
	int newFd = - this->sockfd;

	SlaveSocket::slaves->replaceKey( this->sockfd, newFd );
	Socket::stop();
}

ssize_t SlaveSocket::send( char *buf, size_t ulen, bool &connected ) {
	return Socket::send( this->sockfd, buf, ulen, connected );
}

ssize_t SlaveSocket::recv( char *buf, size_t ulen, bool &connected, bool wait ) {
	return Socket::recv( this->sockfd, buf, ulen, connected, wait );
}

ssize_t SlaveSocket::recvRem( char *buf, size_t expected, char *prevBuf, size_t prevSize, bool &connected ) {
	return Socket::recvRem( this->sockfd, buf, expected, prevBuf, prevSize, connected );
}

bool SlaveSocket::done() {
	return Socket::done( this->sockfd );
}

bool SlaveSocket::ready() {
	return this->connected && this->registered;
}

void SlaveSocket::print( FILE *f ) {
	char buf[ 16 ];
	Socket::ntoh_ip( this->addr.sin_addr.s_addr, buf, 16 );
	fprintf( f, "[%4d] %s:%u ", this->sockfd, buf, Socket::ntoh_port( this->addr.sin_port ) );
	if ( this->connected )
		fprintf( f, "(connected %s registered)\n", this->registered ? "and" : "but not" );
	else
		fprintf( f, "(disconnected)\n" );
}
