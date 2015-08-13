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
	this->identifier = 0;
}

bool SlaveSocket::init( int type, unsigned long addr, unsigned short port, char *name, EPoll *epoll ) {
	this->epoll = epoll;
	this->identifier = strdup( name );
	return (
		Socket::init( type, addr, port ) &&
		this->listen() &&
		epoll->add( this->sockfd, EPOLL_EVENT_SET )
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
				socket->protocol.parseHeader( header, socket->buffer.data, socket->buffer.size );
				// Register message expected
				if ( header.magic == PROTO_MAGIC_REQUEST && header.opcode == PROTO_OPCODE_REGISTER ) {
					if ( header.from == PROTO_MAGIC_FROM_MASTER ) {
						MasterSocket masterSocket;
						masterSocket.init( fd, *addr );
						slave->sockets.masters.set( fd, masterSocket );

						MasterEvent event;
						event.resRegister( slave->sockets.masters.get( fd ) );
						slave->eventQueue.insert( event );
					} else if ( header.from == PROTO_MAGIC_FROM_SLAVE ) {
						// Receive remaining message
						char message[ SERVER_ADDR_MESSSAGE_MAX_LEN ];
						SlavePeerSocket *s = 0;
						ServerAddr serverAddr;

						if ( header.length > SERVER_ADDR_MESSSAGE_MAX_LEN ) {
							__ERROR__( "SlaveSocket", "handler", "Unexpected header size in slave registration." );
							goto slave_peer_recv_error;
						}

						ret = socket->recv( fd, message, header.length, connected, true );
						if ( ret < 0 || ret != header.length ) {
							__ERROR__( "SlaveSocket", "handler", "Cannot receive message." );
							goto slave_peer_recv_error;
						}

						// Find the corresponding SlavePeerSocket
						serverAddr.deserialize( message );
						for ( int i = 0, len = slave->sockets.slavePeers.size(); i < len; i++ ) {
							if ( slave->sockets.slavePeers[ i ].isMatched( serverAddr ) ) {
								s = &slave->sockets.slavePeers[ i ];
								int oldFd = s->getSocket();
								slave->sockets.slavePeers.replaceKey( oldFd, fd );
								s->setRecvFd( fd, addr );
								break;
							}
						}

						if ( s ) {
							SlavePeerEvent event;
							event.resRegister( s, true );
							slave->eventQueue.insert( event );
						} else {
slave_peer_recv_error:
							__ERROR__( "SlaveSocket", "handler", "Unexpected registration from slave." );
							socket->sockets.removeAt( index );
							::close( fd );
							return false;
						}
					} else {
						socket->sockets.removeAt( index );
						::close( fd );
						__ERROR__( "SlaveSocket", "handler", "Invalid register message source." );
						return false;
					}
					socket->sockets.removeAt( index );
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
