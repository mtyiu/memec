#include <cerrno>
#include "coordinator_socket.hh"
#include "../main/coordinator.hh"
#include "../../common/ds/instance_id_generator.hh"
#include "../../common/util/debug.hh"

#define SOCKET_COLOR YELLOW

CoordinatorSocket::CoordinatorSocket() {
	this->isRunning = false;
	this->tid = 0;
	this->epoll = 0;
	this->buffer.size = PROTO_HEADER_SIZE + PROTO_ADDRESS_SIZE;
	this->sockets.needsDelete = true;
}

bool CoordinatorSocket::init( int type, uint32_t addr, uint16_t port, int numSlaves, EPoll *epoll ) {
	this->epoll = epoll;
	bool ret = (
		Socket::init( type, addr, port ) &&
		this->listen() &&
		epoll->add( this->sockfd, EPOLL_EVENT_LISTEN )
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

void CoordinatorSocket::print( FILE *f ) {
	char buf[ 16 ];
	Socket::ntoh_ip( this->addr.sin_addr.s_addr, buf, 16 );
	fprintf( f, "[%4d] %s:%u (%slistening)\n", this->sockfd, buf, Socket::ntoh_port( this->addr.sin_port ), this->isRunning ? "" : "not " );
}

void CoordinatorSocket::printThread( FILE *f ) {
	fprintf( f, "CoordinatorSocket thread for epoll (#%lu): %srunning\n", this->tid, this->isRunning ? "" : "not " );
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
	static InstanceIdGenerator *generator = InstanceIdGenerator::getInstance();

	///////////////////////////////////////////////////////////////////////////
	if ( ! ( events & EPOLLIN ) && ( ( events & EPOLLERR ) || ( events & EPOLLHUP ) || ( events & EPOLLRDHUP ) ) ) {
		// Find the socket in the lists
		int index;
		if ( socket->sockets.get( fd, &index ) ) {
			::close( fd );
			socket->sockets.removeAt( index );
		} else {
			MasterSocket *masterSocket = coordinator->sockets.masters.get( fd );
			SlaveSocket *slaveSocket = masterSocket ? 0 : coordinator->sockets.slaves.get( fd );
			slaveSocket = slaveSocket ? slaveSocket : coordinator->sockets.backupSlaves.get( fd );
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
		struct sockaddr_in *addr;
		socklen_t addrlen;
		while( 1 ) {
			addr = new struct sockaddr_in;
			fd = socket->accept( addr, &addrlen );
			if ( fd == -1 ) {
				delete addr;
				if ( errno != EAGAIN && errno != EWOULDBLOCK ) {
					__ERROR__( "CoordinatorSocket", "handler", "%s", strerror( errno ) );
					return false;
				}
				break;
			}
			CoordinatorSocket::setNonBlocking( fd );
			socket->sockets.set( fd, addr, false );
			socket->epoll->add( fd, EPOLL_EVENT_SET );
		}
	///////////////////////////////////////////////////////////////////////////
	} else {
		int index;
		struct sockaddr_in *addr;
		uint16_t instanceId;

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
				bool ret = socket->protocol.parseHeader( header, socket->buffer.data, socket->buffer.size );
				// Register message expected
				if ( ret && header.magic == PROTO_MAGIC_REQUEST && header.opcode == PROTO_OPCODE_REGISTER ) {
					struct AddressHeader addressHeader;
					socket->protocol.parseAddressHeader( addressHeader, socket->buffer.data + PROTO_HEADER_SIZE, socket->buffer.size - PROTO_HEADER_SIZE );
					if ( header.from == PROTO_MAGIC_FROM_CLIENT ) {
						MasterSocket *masterSocket = new MasterSocket();
						masterSocket->init( fd, *addr );
						masterSocket->setListenAddr( addressHeader.addr, addressHeader.port );
						coordinator->sockets.masters.set( fd, masterSocket );
						socket->sockets.removeAt( index );

						socket->done( fd ); // The socket is valid

						MasterEvent event;
						instanceId = generator->generate( masterSocket );
						event.resRegister( masterSocket, instanceId, header.requestId );
						coordinator->eventQueue.insert( event );
					} else if ( header.from == PROTO_MAGIC_FROM_SERVER ) {
						SlaveSocket *s = 0;

						for ( int i = 0, len = coordinator->sockets.slaves.size(); i < len; i++ ) {
							if ( coordinator->sockets.slaves[ i ]->equal( addressHeader.addr, addressHeader.port ) ) {
								s = coordinator->sockets.slaves[ i ];
								int oldFd = s->getSocket();
								coordinator->sockets.slaves.replaceKey( oldFd, fd );
								s->setRecvFd( fd, addr );
								socket->sockets.removeAt( index );

								socket->done( fd ); // The socket is valid
								break;
							}
						}

						if ( s ) {
							SlaveEvent event;
							instanceId = generator->generate( s );
							event.resRegister( s, instanceId, header.requestId );
							coordinator->eventQueue.insert( event );

							event.announceSlaveConnected( s );
							coordinator->eventQueue.insert( event );
						} else {
							// Treat it as backup slaves
							ServerAddr serverAddr( "backup", addressHeader.addr, addressHeader.port );
							s = new SlaveSocket();
							s->init( fd, serverAddr, socket->epoll );
							s->setRecvFd( fd, addr );
							coordinator->sockets.backupSlaves.set( fd, s );

							socket->sockets.removeAt( index );
							socket->done( fd );

							SlaveEvent event;
							instanceId = generator->generate( s );
							event.resRegister( s, instanceId, header.requestId );
							coordinator->eventQueue.insert( event );

							// __ERROR__( "CoordinatorSocket", "handler", "Unexpected registration from slave." );
							// socket->sockets.removeAt( index );
							// ::close( fd );
							// return false;
						}
					} else {
						::close( fd );
						socket->sockets.removeAt( index );
						__ERROR__( "CoordinatorSocket", "handler", "Invalid register message source." );
						return false;
					}
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
			slaveSocket = slaveSocket ? slaveSocket : coordinator->sockets.backupSlaves.get( fd );
			if ( masterSocket ) {
				MasterEvent event;
				event.pending( masterSocket );
				coordinator->eventQueue.insert( event );
			} else if ( slaveSocket ) {
				SlaveEvent event;
				event.pending( slaveSocket );
				coordinator->eventQueue.insert( event );
			} else {
				__ERROR__( "CoordinatorSocket", "handler", "Unknown socket (fd = %d).", fd );
				return false;
			}
		}
	}
	return true;
}
