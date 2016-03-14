#include <cstring>
#include "simple_remap_msg_handler.hh"

SimpleRemapMsgHandler::SimpleRemapMsgHandler() :
	 SimpleRemapMsgHandler( ( char * )CLIENT_GROUP ) {
}

SimpleRemapMsgHandler::SimpleRemapMsgHandler( char *group ) :
		RemapMsgHandler() {
	this->group = group;
	this->clients = 0;
	this->servers = 0;
}

SimpleRemapMsgHandler::~SimpleRemapMsgHandler() {
}

bool SimpleRemapMsgHandler::start() {
	if ( ! this->isConnected )
		return false;
	if ( pthread_create( &this->reader, NULL, SimpleRemapMsgHandler::readMessages, this ) < 0 ) {
		fprintf( stderr, "SimpleRemapMsgHandler FAILED to start reading messages\n" );
		return false;
	}
	return true;
}

bool SimpleRemapMsgHandler::stop() {
	return ( pthread_cancel( this->reader ) == 0 );
}

bool SimpleRemapMsgHandler::join( const char *group ) {
	return ( SP_join( this->mbox, group ) );
}

bool SimpleRemapMsgHandler::isClientJoin( int service, char *msg, char *subject ) {
	return ( this->isMemberJoin( service ) && strncmp( subject + 1, CLIENT_PREFIX, CLIENT_PREFIX_LEN ) == 0 );
}

bool SimpleRemapMsgHandler::isServerJoin( int service, char *msg, char *subject ) {
	return ( this->isMemberJoin( service ) && strncmp( subject + 1, SERVER_PREFIX, SERVER_PREFIX_LEN ) == 0 );
}

bool SimpleRemapMsgHandler::addAliveServer( struct sockaddr_in server ) {
	return false;
}

bool SimpleRemapMsgHandler::removeAliveServer( struct sockaddr_in server ) {
	return false;
}

void *SimpleRemapMsgHandler::readMessages( void* argv ) {
	int ret = 0;

	SimpleRemapMsgHandler *myself = ( SimpleRemapMsgHandler * ) argv;

	int service, groups, endian;
    int16 msgType;
    char sender[ MAX_GROUP_NAME ], msg[ MAX_MESSLEN ];
    char targetGroups[ MAX_GROUP_NUM ][ MAX_GROUP_NAME ];
    char* subject;

	bool regular = false, fromClient = false, fromServer = false;

	while ( ret >= 0 ) {
		ret = SP_receive( myself->mbox, &service, sender, MAX_GROUP_NUM, &groups, targetGroups, &msgType, &endian, MAX_MESSLEN, msg );
		if ( ret < 0 ) {
			fprintf( stderr, "[Error] SP_receive returns %d\n", ret );
			continue;
		}
		subject = &msg[ SP_get_vs_set_offset_memb_mess() ];
        regular = myself->isRegularMessage( service );
        fromClient = ( strncmp( sender, CLIENT_GROUP, CLIENT_GROUP_LEN ) == 0 );
        fromServer = ( strncmp( sender, SERVER_GROUP, SERVER_GROUP_LEN ) == 0 );

		if ( regular )
			continue;

		if ( fromClient && myself->isClientJoin( service, msg, subject ) ) {
			myself->clients++;
			//printf( "Client %s joins (%d)\n", subject, ( int ) myself->clients );
		} else if ( fromServer && myself->isServerJoin( service, msg, subject ) ) {
			myself->servers++;
			//printf( "Server %s joins (%d)\n", subject, ( int ) myself->servers );
		} else {
		}
	}

	return 0;
}

int SimpleRemapMsgHandler::sendStatePub ( std::vector<struct sockaddr_in> &servers, int numGroup, const char targetGroup[][ MAX_GROUP_NAME ] ) {
	return sendState( servers, numGroup, targetGroup );
}
