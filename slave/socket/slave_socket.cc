#include <cerrno>
#include "slave_socket.hh"
#include "../main/slave.hh"
#include "../../common/util/debug.hh"

#define SOCKET_COLOR YELLOW

SlaveSocket::SlaveSocket() {
	this->isRunning = false;
	this->tid = 0;
	this->epoll = 0;
	this->buffer.size = PROTO_HEADER_SIZE + PROTO_ADDRESS_SIZE;
	this->identifier = 0;
	this->sockets.needsDelete = true;
}

bool SlaveSocket::init( int type, uint32_t addr, uint16_t port, char *name, EPoll *epoll ) {
	this->epoll = epoll;
	this->identifier = strdup( name );
	return (
		Socket::init( type, addr, port ) &&
		this->listen() &&
		epoll->add( this->sockfd, EPOLL_EVENT_LISTEN )
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
	if ( this->identifier ) {
		::free( this->identifier );
		this->identifier = 0;
	}
}

void SlaveSocket::print( FILE *f ) {
	char buf[ 16 ];
	Socket::ntoh_ip( this->addr.sin_addr.s_addr, buf, 16 );
	fprintf( f, "[%4d] %s:%u (%slistening)\n", this->sockfd, buf, Socket::ntoh_port( this->addr.sin_port ), this->isRunning ? "" : "not " );
}

void SlaveSocket::printThread( FILE *f ) {
	fprintf( f, "SlaveSocket thread for epoll (#%lu): %srunning\n", this->tid, this->isRunning ? "" : "not " );
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
	if ( ! ( events & EPOLLIN ) && ( ( events & EPOLLERR ) || ( events & EPOLLHUP ) || ( events & EPOLLRDHUP ) ) ) {
		// Find the socket in the lists
		int index;
		if ( socket->sockets.get( fd, &index ) ) {
			::close( fd );
			socket->sockets.removeAt( index );
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
		struct sockaddr_in *addr;
		socklen_t addrlen;
		while( 1 ) {
			addr = new struct sockaddr_in;
			fd = socket->accept( addr, &addrlen );
			if ( fd == -1 ) {
				delete addr;
				if ( errno != EAGAIN && errno != EWOULDBLOCK ) {
					__ERROR__( "SlaveSocket", "handler", "%s", strerror( errno ) );
					return false;
				}
				break;
			}
			SlaveSocket::setNonBlocking( fd );
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
				__ERROR__( "SlaveSocket", "handler", "Cannot receive message." );
				return false;
			} else if ( ( size_t ) ret == socket->buffer.size ) {
				ProtocolHeader header;
				bool ret = socket->protocol.parseHeader( header, socket->buffer.data, socket->buffer.size );
				// Register message expected
				if ( ret && header.magic == PROTO_MAGIC_REQUEST && header.opcode == PROTO_OPCODE_REGISTER ) {
					struct AddressHeader addressHeader;
					socket->protocol.parseAddressHeader( addressHeader, socket->buffer.data + PROTO_HEADER_SIZE, socket->buffer.size - PROTO_HEADER_SIZE );
					// Register message expected
					if ( header.from == PROTO_MAGIC_FROM_MASTER ) {
						MasterSocket *masterSocket = new MasterSocket();
						masterSocket->init( fd, *addr );
						// Ignore the address header for master socket
						slave->sockets.masters.set( fd, masterSocket );
						socket->sockets.removeAt( index );

						socket->done( fd ); // The socket is valid

						MasterEvent event;
						event.resRegister( masterSocket, header.id );
						slave->eventQueue.insert( event );
					} else if ( header.from == PROTO_MAGIC_FROM_SLAVE ) {
						SlavePeerSocket *s = 0;

						for ( int i = 0, len = slave->sockets.slavePeers.size(); i < len; i++ ) {
							if ( slave->sockets.slavePeers[ i ]->equal( addressHeader.addr, addressHeader.port ) ) {
								s = slave->sockets.slavePeers[ i ];
								int oldFd = s->getSocket();
								slave->sockets.slavePeers.replaceKey( oldFd, fd );
								s->setRecvFd( fd, addr );
								socket->sockets.removeAt( index );

								socket->done( fd ); // The socket is valid
								break;
							}
						}

						if ( s ) {
							SlavePeerEvent event;
							event.resRegister( s, true, header.id );
							slave->eventQueue.insert( event );
						} else {
							__ERROR__( "SlaveSocket", "handler", "Unexpected registration from slave." );
							socket->sockets.removeAt( index );
							::close( fd );
							return false;
						}
					} else {
						::close( fd );
						socket->sockets.removeAt( index );
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
			int index;
			MasterSocket *masterSocket = slave->sockets.masters.get( fd );
			CoordinatorSocket *coordinatorSocket = masterSocket ? 0 : slave->sockets.coordinators.get( fd );
			SlavePeerSocket *slavePeerSocket = ( masterSocket || coordinatorSocket ) ? 0 : slave->sockets.slavePeers.get( fd, &index );

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
				// __ERROR__( "SlaveSocket", "handler", "Unknown socket: fd = %d.", fd );
				return false;
			}
		}
	}
	return true;
}
