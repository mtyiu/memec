#include "worker.hh"
#include "../main/client.hh"

void ClientWorker::dispatch( CoordinatorEvent event ) {
	bool connected, isSend;
	uint16_t instanceId = Client::instanceId;
	uint32_t requestId;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	if ( event.type != COORDINATOR_EVENT_TYPE_PENDING )
		requestId = ClientWorker::idGenerator->nextVal( this->workerId );

	buffer.data = this->protocol.buffer.send;

	switch( event.type ) {
		case COORDINATOR_EVENT_TYPE_REGISTER_REQUEST:
			buffer.size = this->protocol.generateAddressHeader(
				PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_COORDINATOR,
				PROTO_OPCODE_REGISTER,
				PROTO_UNINITIALIZED_INSTANCE, requestId,
				event.message.address.addr,
				event.message.address.port
			);
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_PUSH_LOAD_STATS:
			// TODO lock the latency when constructing msg ??
			buffer.data = this->protocol.reqPushLoadStats(
				buffer.size,
				instanceId,
				requestId,
				event.message.loading.serverGetLatency,
				event.message.loading.serverSetLatency
			);
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_PENDING:
			isSend = false;
			break;
		default:
			return;
	}

	if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "ClientWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
	} else {
		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();
		ArrayMap<struct sockaddr_in, Latency> getLatency, setLatency;
		struct LoadStatsHeader loadStatsHeader;
		Client *client = Client::getInstance();

		while ( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "ClientWorker" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_COORDINATOR ) {
				__ERROR__( "ClientWorker", "dispatch", "Invalid message source from coordinator." );
			} else {
				bool success;
				switch( header.magic ) {
					case PROTO_MAGIC_RESPONSE_SUCCESS:
						success = true;
						break;
					case PROTO_MAGIC_RESPONSE_FAILURE:
					default:
						success = false;
						break;
				}

				event.instanceId = header.instanceId;
				event.requestId = header.requestId;
				switch( header.opcode ) {
					case PROTO_OPCODE_REGISTER:
						switch( header.magic ) {
							case PROTO_MAGIC_RESPONSE_SUCCESS:
								event.socket->registered = true;
								Client::instanceId = header.instanceId;
								break;
							case PROTO_MAGIC_RESPONSE_FAILURE:
								__ERROR__( "ClientWorker", "dispatch", "Failed to register with coordinator." );
								break;
							case PROTO_MAGIC_LOADING_STATS:
								this->protocol.parseLoadStatsHeader( loadStatsHeader, buffer.data, buffer.size );
								buffer.data += PROTO_LOAD_STATS_SIZE;
								buffer.size -= PROTO_LOAD_STATS_SIZE;

								// parse the loading stats and merge with existing stats
								LOCK( &client->overloadedServer.lock );
								client->overloadedServer.serverSet.clear();
								this->protocol.parseLoadingStats( loadStatsHeader, getLatency, setLatency, client->overloadedServer.serverSet, buffer.data, buffer.size );
								UNLOCK( &client->overloadedServer.lock );
								client->mergeServerCumulativeLoading( &getLatency, &setLatency );

								buffer.data -= PROTO_LOAD_STATS_SIZE;
								buffer.size += PROTO_LOAD_STATS_SIZE;

								getLatency.needsDelete = false;
								setLatency.needsDelete = false;
								getLatency.clear();
								setLatency.clear();

								break;
							default:
								__ERROR__( "ClientWorker", "dispatch", "Invalid magic code from coordinator." );
								break;
						}
						break;
					case PROTO_OPCODE_DEGRADED_LOCK:
						this->handleDegradedLockResponse( event, success, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_REMAPPING_LOCK:
						switch( header.magic ) {
							case PROTO_MAGIC_RESPONSE_SUCCESS:
								success = true;
								break;
							case PROTO_MAGIC_RESPONSE_FAILURE:
								success = false;
								break;
							default:
								__ERROR__( "ClientWorker", "dispatch", "Invalid magic code from coordinator." );
								goto quit_1;
						}
						this->handleDegradedSetLockResponse( event, success, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_SERVER_RECONSTRUCTED:
						this->handleServerReconstructedMsg( event, buffer.data, header.length );
						break;
					case PROTO_OPCODE_ADD_NEW_SERVER:
						this->handleAddNewServerRequest( event, buffer.data, header.length );
						break;
					case PROTO_OPCODE_STRIPE_LIST_UPDATE:
						this->handleStripeListUpdateRequest( event, buffer.data, header.length );
						break;
					default:
						__ERROR__( "ClientWorker", "dispatch", "Invalid opcode from coordinator." );
						break;
				}
			}
quit_1:
			buffer.data += header.length;
			buffer.size -= header.length;
		}

		if ( connected ) event.socket->done();
	}
	if ( ! connected )
		__ERROR__( "ClientWorker", "dispatch", "The coordinator is disconnected." );
}

bool ClientWorker::handleAddNewServerRequest( CoordinatorEvent event, char *buf, size_t size ) {
	struct NewServerHeader header;
	char ipStr[ 16 ], portStr[ 6 ];
	if ( ! this->protocol.parseNewServerHeader( header, buf, size ) ) {
		__ERROR__( "ClientWorker", "handleAddNewServerRequest", "Invalid new server header." );
		return false;
	}

	Socket::ntoh_ip( header.addr, ipStr, sizeof( ipStr ) );
	Socket::ntoh_port( header.port, portStr, sizeof( portStr ) );
	__INFO__(
		BLUE, "ClientWorker", "handleAddNewServerRequest",
		"Server name: %.*s (length = %u); address: %s; port: %s.",
		header.length, header.name, header.length,
		ipStr, portStr
	);

	char backup = header.name[ header.length ];
	header.name[ header.length ] = 0;
	ServerAddr addr( header.name, header.addr, header.port );
	header.name[ header.length ] = backup;

	// Update global config
	Client *client = Client::getInstance();
	size_t index = client->config.global.servers.size();
	client->config.global.servers.push_back( addr );

	// Add new server socket
	ServerSocket *socket = new ServerSocket();
	socket->init(
		client->config.global.servers[ index ],
		&client->sockets.epoll
	);
	int fd = socket->getSocket();
	client->sockets.servers.set( fd, socket );

	client->stripeList->addNewServer( socket );

	client->stripeList->print( stderr, false );
	client->stripeList->print( stderr, true );

	return true;
}

bool ClientWorker::handleStripeListUpdateRequest( CoordinatorEvent event, char *buf, size_t size ) {
	struct StripeListScalingHeader header;
	struct StripeListPartitionHeader list;
	size_t next = 0;
	if ( ! this->protocol.parseStripeListScalingHeader( header, next, buf, size ) ) {
		__ERROR__( "ClientWorker", "handleStripeListUpdateRequest", "Invalid new server header." );
		return false;
	}
	__INFO__(
		BLUE, "ClientWorker", "handleStripeListUpdateRequest",
		"Is migrating? %s; number of servers = %u, number of lists = %u, n = %u, k = %u.",
		header.isMigrating ? "yes" : "no",
		header.numServers, header.numLists,
		header.n, header.k
	);

	for ( uint32_t i = 0; i < header.numLists; i++ ) {
		this->protocol.parseStripeListPartitionHeader(
			list, next,
			header.n, header.k,
			buf, size, next
		);

		__INFO__(
			BLUE, "ClientWorker", "handleStripeListUpdateRequest",
			"List #%3u [%10u-%10u]:",
			list.listId, list.partitionFrom, list.partitionTo
		);
		for ( uint32_t j = 0; j < header.n; j++ ) {
			fprintf( stderr, "%u ", j < header.k ? list.data[ j ] : list.parity[ j - header.k ] );
		}
		fprintf( stderr, "\n" );
	}

	return true;
}
