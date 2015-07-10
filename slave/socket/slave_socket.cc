#include <cerrno>
#include "slave_socket.hh"
#include "../main/slave.hh"
#include "../../common/util/debug.hh"

#define SOCKET_COLOR YELLOW

SlaveSocket::SlaveSocket() {
	this->isRunning = false;
	this->tid = 0;
	this->epoll = 0;
	this->buffer.size = PROTO_HEADER_SIZE;
}

bool SlaveSocket::init( int type, unsigned long addr, unsigned short port, EPoll *epoll ) {
	this->epoll = epoll;
	return (
		Socket::init( type, addr, port ) &&
		this->listen() &&
		epoll->add( this->sockfd, EPOLLIN | EPOLLET )
	);
}

bool SlaveSocket::start() {
	if ( pthread_create( &this->tid, NULL, SlaveSocket::run, ( void * ) this ) != 0 ) {
		__ERROR__( "SlaveSocket", "start", "Cannot start SlaveSocket thread." );
		return false;
	}
	this->isRunning = true;
	return true;
}

void SlaveSocket::stop() {
	if ( this->isRunning ) {
		this->epoll->stop( this->tid );
		this->isRunning = false;
		pthread_join( this->tid, 0 );
	}
}

void SlaveSocket::debug() {
	__DEBUG__( SOCKET_COLOR, "SlaveSocket", "debug", "SlaveSocket thread for epoll #%lu is %srunning.", this->tid, this->isRunning ? "" : "not " );
}

void *SlaveSocket::run( void *argv ) {
	SlaveSocket *socket = ( SlaveSocket * ) argv;
	socket->epoll->start( SlaveSocket::handler, socket );
	pthread_exit( 0 );
	return 0;
}

bool SlaveSocket::handler( int fd, uint32_t events, void *data ) {
	SlaveSocket *socket = ( SlaveSocket * ) data;
	static Slave *slave = Slave::getInstance();

	///////////////////////////////////////////////////////////////////////////
	if ( ( events & EPOLLERR ) || ( events & EPOLLHUP ) || ( events & EPOLLRDHUP ) ) {
		// Find the socket in the lists
		int index;
		if ( socket->sockets.get( fd, &index ) ) {
			socket->sockets.removeAt( index );
			::close( fd );
		} else {
			MasterSocket *masterSocket = slave->sockets.masters.get( fd );
			CoordinatorSocket *coordinatorSocket = masterSocket ? 0 : slave->sockets.coordinators.get( fd );
			SlavePeerSocket *slavePeerSocket = ( masterSocket || coordinatorSocket ) ? 0 : slave->sockets.slavePeers.get( fd );
			if ( masterSocket ) {
				masterSocket->stop();
			} else if ( coordinatorSocket ) {
				coordinatorSocket->stop();
			} else if ( slavePeerSocket ) {
				slavePeerSocket->stop();
			} else {
				__ERROR__( "SlaveSocket", "handler", "Unknown socket." );
				return false;
			}
		}
	///////////////////////////////////////////////////////////////////////////
	} else if ( fd == socket->getSocket() ) {
		struct sockaddr_in addr;
		socklen_t addrlen;
		while( 1 ) {
			fd = socket->accept( &addr, &addrlen );
			if ( fd == -1 ) {
				if ( errno != EAGAIN && errno != EWOULDBLOCK ) {
					__ERROR__( "SlaveSocket", "handler", "%s", strerror( errno ) );
					return false;
				}
				break;
			}
			socket->sockets.set( fd, addr, false );
			socket->epoll->add( fd, EPOLLIN | EPOLLRDHUP | EPOLLET );
		}
	///////////////////////////////////////////////////////////////////////////
	} else {
		int index;
		struct sockaddr_in *addr;
		if ( ( addr = socket->sockets.get( fd, &index ) ) ) {
			// Read message immediately and add to appropriate socket list such that all "add" operations originate from the epoll thread
			// Only master or slave register message is expected
			bool connected;
			ssize_t ret;
			
			ret = socket->recv( fd, socket->buffer.data, socket->buffer.size, connected, true );
			if ( ret < 0 ) {
				__ERROR__( "SlaveSocket", "handler", "Cannot receive message." );
				return false;
			} else if ( ( size_t ) ret == socket->buffer.size ) {
				uint8_t magic, from, opcode;
				uint32_t length;
				socket->protocol.parseHeader( socket->buffer.data, socket->buffer.size, magic, from, opcode, length );
				// Register message expected
				if ( magic == PROTO_MAGIC_REQUEST && opcode == PROTO_OPCODE_REGISTER ) {
					if ( from == PROTO_MAGIC_FROM_MASTER ) {
						MasterSocket masterSocket;
						masterSocket.init( fd, *addr );
						slave->sockets.masters.set( fd, masterSocket );

						MasterEvent event;
						event.resRegister( slave->sockets.masters.get( fd ) );
						slave->eventQueue.insert( event );
					} else if ( from == PROTO_MAGIC_FROM_SLAVE ) {
						SlavePeerSocket slavePeerSocket;
						slavePeerSocket.init( fd, *addr );
						slave->sockets.slavePeers.set( fd, slavePeerSocket );

						SlavePeerEvent event;
						event.resRegister( slave->sockets.slavePeers.get( fd ) );
						slave->eventQueue.insert( event );
					} else {
						__ERROR__( "SlaveSocket", "handler", "Invalid register message source." );
						return false;
					}
				} else {
					__ERROR__( "SlaveSocket", "handler", "Invalid register message." );
					return false;
				}
			} else {
				__ERROR__( "SlaveSocket", "handler", "Message corrupted." );
				return false;
			}
		} else {
			MasterSocket *masterSocket = slave->sockets.masters.get( fd );
			CoordinatorSocket *coordinatorSocket = masterSocket ? 0 : slave->sockets.coordinators.get( fd );
			SlavePeerSocket *slavePeerSocket = ( masterSocket || coordinatorSocket ) ? 0 : slave->sockets.slavePeers.get( fd );
			if ( masterSocket ) {
				MasterEvent event;
				event.pending( masterSocket );
				slave->eventQueue.insert( event );
			} else if ( coordinatorSocket ) {
				CoordinatorEvent event;
				event.pending( coordinatorSocket );
				slave->eventQueue.insert( event );
			} else if ( slavePeerSocket ) {
				SlavePeerEvent event;
				event.pending( slavePeerSocket );
				slave->eventQueue.insert( event );
			} else {
				__ERROR__( "SlaveSocket", "handler", "Unknown socket." );
				return false;
			}
		}
	}
	return true;
}
