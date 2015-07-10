#include <cerrno>
#include "coordinator_socket.hh"
#include "../main/coordinator.hh"
#include "../../common/util/debug.hh"

#define SOCKET_COLOR YELLOW

CoordinatorSocket::CoordinatorSocket() {
	this->isRunning = false;
	this->tid = 0;
	this->epoll = 0;
	this->buffer.size = PROTO_HEADER_SIZE;
}

bool CoordinatorSocket::init( int type, unsigned long addr, unsigned short port, int numSlaves, EPoll *epoll ) {
	this->epoll = epoll;
	bool ret = (
		Socket::init( type, addr, port ) &&
		this->listen() &&
		epoll->add( this->sockfd, EPOLL_EVENT_SET )
	);
	if ( ret ) {
		this->sockets.reserve( numSlaves );
	}
	return ret;
}

bool CoordinatorSocket::start() {
	if ( pthread_create( &this->tid, NULL, CoordinatorSocket::run, ( void * ) this ) != 0 ) {
		__ERROR__( "CoordinatorSocket", "start", "Cannot start CoordinatorSocket thread." );
		return false;
	}
	this->isRunning = true;
	return true;
}

void CoordinatorSocket::stop() {
	if ( this->isRunning ) {
		this->epoll->stop( this->tid );
		this->isRunning = false;
		pthread_join( this->tid, 0 );
	}
}

void CoordinatorSocket::debug() {
	__DEBUG__( SOCKET_COLOR, "CoordinatorSocket", "debug", "CoordinatorSocket thread for epoll #%lu is %srunning.", this->tid, this->isRunning ? "" : "not " );
}

void *CoordinatorSocket::run( void *argv ) {
	CoordinatorSocket *socket = ( CoordinatorSocket * ) argv;
	socket->epoll->start( CoordinatorSocket::handler, socket );
	pthread_exit( 0 );
	return 0;
}

bool CoordinatorSocket::handler( int fd, uint32_t events, void *data ) {
	CoordinatorSocket *socket = ( CoordinatorSocket * ) data;
	static Coordinator *coordinator = Coordinator::getInstance();

	///////////////////////////////////////////////////////////////////////////
	if ( ( events & EPOLLERR ) || ( events & EPOLLHUP ) || ( events & EPOLLRDHUP ) ) {
		// Find the socket in the lists
		int index;
		if ( socket->sockets.get( fd, &index ) ) {
			socket->sockets.removeAt( index );
			::close( fd );
		} else {
			MasterSocket *masterSocket = coordinator->sockets.masters.get( fd );
			SlaveSocket *slaveSocket = masterSocket ? 0 : coordinator->sockets.slaves.get( fd );
			if ( masterSocket ) {
				masterSocket->stop();
			} else if ( slaveSocket ) {
				slaveSocket->stop();
			} else {
				__ERROR__( "CoordinatorSocket", "handler", "Unknown socket." );
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
					__ERROR__( "CoordinatorSocket", "handler", "%s", strerror( errno ) );
					return false;
				}
				break;
			}
			socket->sockets.set( fd, addr, false );
			socket->epoll->add( fd, EPOLL_EVENT_SET );
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
				__ERROR__( "CoordinatorSocket", "handler", "Cannot receive message." );
				return false;
			} else if ( ( size_t ) ret == socket->buffer.size ) {
				ProtocolHeader header;
				socket->protocol.parseHeader( socket->buffer.data, socket->buffer.size, header );
				// Register message expected
				if ( header.magic == PROTO_MAGIC_REQUEST && header.opcode == PROTO_OPCODE_REGISTER ) {
					if ( header.from == PROTO_MAGIC_FROM_MASTER ) {
						MasterSocket masterSocket;
						masterSocket.init( fd, *addr );
						coordinator->sockets.masters.set( fd, masterSocket );

						MasterEvent event;
						event.resRegister( coordinator->sockets.masters.get( fd ) );
						coordinator->eventQueue.insert( event );
					} else if ( header.from == PROTO_MAGIC_FROM_SLAVE ) {
						SlaveSocket slaveSocket;
						slaveSocket.init( fd, *addr );
						coordinator->sockets.slaves.set( fd, slaveSocket );

						SlaveEvent event;
						event.resRegister( coordinator->sockets.slaves.get( fd ) );
						coordinator->eventQueue.insert( event );
					} else {
						socket->sockets.removeAt( index );
						::close( fd );
						__ERROR__( "CoordinatorSocket", "handler", "Invalid register message source." );
						return false;
					}
					socket->sockets.removeAt( index );
				} else {
					__ERROR__( "CoordinatorSocket", "handler", "Invalid register message." );
					return false;
				}
			} else {
				__ERROR__( "CoordinatorSocket", "handler", "Message corrupted." );
				return false;
			}
		} else {
			MasterSocket *masterSocket = coordinator->sockets.masters.get( fd );
			SlaveSocket *slaveSocket = masterSocket ? 0 : coordinator->sockets.slaves.get( fd );
			if ( masterSocket ) {
				MasterEvent event;
				event.pending( masterSocket );
				coordinator->eventQueue.insert( event );
			} else if ( slaveSocket ) {
				SlaveEvent event;
				event.pending( slaveSocket );
				coordinator->eventQueue.insert( event );
			} else {
				__ERROR__( "CoordinatorSocket", "handler", "Unknown socket." );
				return false;
			}
		}
	}
	return true;
}
