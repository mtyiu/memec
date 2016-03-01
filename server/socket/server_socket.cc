#include <cerrno>
#include "server_socket.hh"
#include "../main/server.hh"
#include "../../common/util/debug.hh"

#define SOCKET_COLOR YELLOW

ServerSocket::ServerSocket() {
	this->isRunning = false;
	this->tid = 0;
	this->epoll = 0;
	this->buffer.size = PROTO_HEADER_SIZE + PROTO_ADDRESS_SIZE;
	this->identifier = 0;
	this->sockets.needsDelete = true;
}

bool ServerSocket::init( int type, uint32_t addr, uint16_t port, char *name, EPoll *epoll ) {
	this->epoll = epoll;
	this->identifier = strdup( name );
	return (
		Socket::init( type, addr, port ) &&
		this->listen() &&
		epoll->add( this->sockfd, EPOLL_EVENT_LISTEN )
	);
}

bool ServerSocket::start() {
	if ( pthread_create( &this->tid, NULL, ServerSocket::run, ( void * ) this ) != 0 ) {
		__ERROR__( "ServerSocket", "start", "Cannot start ServerSocket thread." );
		return false;
	}
	this->isRunning = true;
	return true;
}

void ServerSocket::stop() {
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

void ServerSocket::print( FILE *f ) {
	char buf[ 16 ];
	Socket::ntoh_ip( this->addr.sin_addr.s_addr, buf, 16 );
	fprintf( f, "[%4d] %s:%u (%slistening)\n", this->sockfd, buf, Socket::ntoh_port( this->addr.sin_port ), this->isRunning ? "" : "not " );
}

void ServerSocket::printThread( FILE *f ) {
	fprintf( f, "ServerSocket thread for epoll (#%lu): %srunning\n", this->tid, this->isRunning ? "" : "not " );
}

void *ServerSocket::run( void *argv ) {
	ServerSocket *socket = ( ServerSocket * ) argv;
	socket->epoll->start( ServerSocket::handler, socket );
	pthread_exit( 0 );
	return 0;
}

bool ServerSocket::handler( int fd, uint32_t events, void *data ) {
	ServerSocket *socket = ( ServerSocket * ) data;
	static Slave *slave = Slave::getInstance();

	///////////////////////////////////////////////////////////////////////////
	if ( ! ( events & EPOLLIN ) && ( ( events & EPOLLERR ) || ( events & EPOLLHUP ) || ( events & EPOLLRDHUP ) ) ) {
		// Find the socket in the lists
		int index;
		if ( socket->sockets.get( fd, &index ) ) {
			::close( fd );
			socket->sockets.removeAt( index );
		} else {
			ClientSocket *clientSocket = slave->sockets.masters.get( fd );
			CoordinatorSocket *coordinatorSocket = clientSocket ? 0 : slave->sockets.coordinators.get( fd );
			ServerPeerSocket *serverPeerSocket = ( clientSocket || coordinatorSocket ) ? 0 : slave->sockets.slavePeers.get( fd );
			if ( clientSocket ) {
				clientSocket->stop();
			} else if ( coordinatorSocket ) {
				coordinatorSocket->stop();
			} else if ( serverPeerSocket ) {
				serverPeerSocket->stop();
			} else {
				__ERROR__( "ServerSocket", "handler", "Unknown socket." );
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
					__ERROR__( "ServerSocket", "handler", "%s", strerror( errno ) );
					return false;
				}
				break;
			}
			ServerSocket::setNonBlocking( fd );
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
				__ERROR__( "ServerSocket", "handler", "Cannot receive message." );
				return false;
			} else if ( ( size_t ) ret == socket->buffer.size ) {
				ProtocolHeader header;
				bool ret = socket->protocol.parseHeader( header, socket->buffer.data, socket->buffer.size );
				// Register message expected
				if ( ret && header.magic == PROTO_MAGIC_REQUEST && header.opcode == PROTO_OPCODE_REGISTER ) {
					struct AddressHeader addressHeader;
					socket->protocol.parseAddressHeader( addressHeader, socket->buffer.data + PROTO_HEADER_SIZE, socket->buffer.size - PROTO_HEADER_SIZE );
					// Register message expected
					if ( header.from == PROTO_MAGIC_FROM_CLIENT ) {
						ClientSocket *clientSocket = new ClientSocket();
						clientSocket->init( fd, *addr );
						// Ignore the address header for master socket
						slave->sockets.masters.set( fd, clientSocket );
						// add socket to the instance-id-to-master-socket mapping
						LOCK( &slave->sockets.mastersIdToSocketLock );
						slave->sockets.mastersIdToSocketMap[ header.instanceId ] = clientSocket;
						UNLOCK( &slave->sockets.mastersIdToSocketLock );
						socket->sockets.removeAt( index );

						socket->done( fd ); // The socket is valid

						ClientEvent event;
						event.resRegister( clientSocket, header.instanceId, header.requestId );
						slave->eventQueue.insert( event );
					} else if ( header.from == PROTO_MAGIC_FROM_SERVER ) {
						ServerPeerSocket *s = 0;

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
							ServerPeerEvent event;
							event.resRegister( s, true, header.instanceId, header.requestId );
							slave->eventQueue.insert( event );
						} else {
							__ERROR__( "ServerSocket", "handler", "Unexpected registration from slave." );
							socket->sockets.removeAt( index );
							::close( fd );
							return false;
						}
					} else {
						::close( fd );
						socket->sockets.removeAt( index );
						__ERROR__( "ServerSocket", "handler", "Invalid register message source." );
						return false;
					}
				} else {
					__ERROR__( "ServerSocket", "handler", "Invalid register message." );
					return false;
				}
			} else {
				__ERROR__( "ServerSocket", "handler", "Message corrupted." );
				return false;
			}
		} else {
			int index;
			ClientSocket *clientSocket = slave->sockets.masters.get( fd );
			CoordinatorSocket *coordinatorSocket = clientSocket ? 0 : slave->sockets.coordinators.get( fd );
			ServerPeerSocket *serverPeerSocket = ( clientSocket || coordinatorSocket ) ? 0 : slave->sockets.slavePeers.get( fd, &index );

			if ( clientSocket ) {
				ClientEvent event;
				event.pending( clientSocket );
				slave->eventQueue.insert( event );
			} else if ( coordinatorSocket ) {
				CoordinatorEvent event;
				event.pending( coordinatorSocket );
				slave->eventQueue.insert( event );
			} else if ( serverPeerSocket ) {
				ServerPeerEvent event;
				event.pending( serverPeerSocket );
				slave->eventQueue.insert( event );
			} else {
				// __ERROR__( "ServerSocket", "handler", "Unknown socket: fd = %d.", fd );
				return false;
			}
		}
	}
	return true;
}
