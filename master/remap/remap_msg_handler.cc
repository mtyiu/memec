#include <cstdlib>
#include <arpa/inet.h>
#include <pthread.h>
#include <string.h>
#include "../../common/remap/remap_group.hh"
#include "../main/master.hh"
#include "remap_msg_handler.hh"

MasterRemapMsgHandler::MasterRemapMsgHandler() :
        RemapMsgHandler() {
    this->group = ( char* )MASTER_GROUP;
}

MasterRemapMsgHandler::~MasterRemapMsgHandler() {
}

bool MasterRemapMsgHandler::init( const int ip, const int port, const char *user ) {
    char addrbuf[ 32 ], ipstr[ INET_ADDRSTRLEN ];
    struct in_addr addr;
    memset( addrbuf, 0, 32 );
    addr.s_addr = ip;
    inet_ntop( AF_INET, &addr, ipstr, INET_ADDRSTRLEN );
    sprintf( addrbuf, "%u@%s", ntohs( port ), ipstr );
    return RemapMsgHandler::init( addrbuf , user ) ;
}

void MasterRemapMsgHandler::quit() {
    RemapMsgHandler::quit();
    if ( this->isListening ) {
        this->stop();
    }
    pthread_join( this->reader, NULL );
    this->isListening = true;
    this->reader = -1;
}

bool MasterRemapMsgHandler::start() {
    if ( ! this->isConnected )
        return false;

    // read message using a background thread
    if ( pthread_create( &this->reader, NULL, MasterRemapMsgHandler::readMessages, this ) < 0 ){
        fprintf( stderr, "Master FAILED to start reading messages\n" );
        return false;
    }
    this->isListening = true;

    return true;
}

bool MasterRemapMsgHandler::stop() {
    int ret = 0;
    if ( ! this->isConnected || ! this->isListening )
        return false;

    // stop reading messages
    this->isListening = false;
    // avoid blocking call from blocking the stop action
    ret = pthread_cancel( this->reader );

    return ( ret == 0 );
}
void *MasterRemapMsgHandler::readMessages( void *argv ) {
    MasterRemapMsgHandler *myself = ( MasterRemapMsgHandler* ) argv;
    int ret = 0;

    int service, groups, endian;
    int16 msgType;
    char sender[ MAX_GROUP_NAME ], msg[ MAX_MESSLEN ];
    char targetGroups[ MAX_GROUP_NUM ][ MAX_GROUP_NAME ];

    // handler messages
    while ( myself->isListening && ret >= 0 ) {
        ret = SP_receive( myself->mbox, &service, sender, MAX_GROUP_NUM, &groups, targetGroups, &msgType, &endian, MAX_MESSLEN, msg);
        if ( ret > 0 && myself->isRegularMessage( service ) ) {
            // change status accordingly
            myself->setStatus( msg, ret );
            myself->increMsgCount();
        }
    }
    if ( ret < 0 ) {
        fprintf( stderr, "master reader extis with error code %d\n", ret );
    }

    return ( void* ) &myself->msgCount;
}

void MasterRemapMsgHandler::setStatus( char* msg , int len ) {
    RemapStatus signal = ( RemapStatus ) atoi(msg);

    switch ( signal ) {
        case REMAP_PREPARE_START:
            printf( "REMAP_PREPARE_START\n" );
            break;
        case REMAP_START:
            printf( "REMAP_START\n" );
            break;
        case REMAP_PREPARE_END:
            printf( "REMAP_PREPARE_END\n" );
            break;
        case REMAP_END:
            printf( "REMAP_END\n" );
            signal = REMAP_NONE;
            break;
        default:
            printf( "REMAP_%d\n", signal );
            return;
    }

    pthread_rwlock_wrlock( &this->stlock );
    this->status = signal;
    pthread_rwlock_unlock( &this->stlock );
    if ( signal == REMAP_PREPARE_START || signal == REMAP_PREPARE_END )
        this->ackRemap( Master::getInstance()->counter.getNormal(), Master::getInstance()->counter.getRemapping() );
}

bool MasterRemapMsgHandler::useRemapFlow() {
	switch ( this->status ) {
		case REMAP_PREPARE_START:
		case REMAP_WAIT_START:
		case REMAP_START:
			return true;
		default:
			break;
	}
	return false;
}

bool MasterRemapMsgHandler::ackRemap( uint32_t normal, uint32_t remapping ) {
    char buf[ MAX_MESSLEN ];
    int len = 0, ret = -1;
    RemapStatus signal = REMAP_UNDEFINED;

    pthread_rwlock_wrlock( &this->stlock );
	if ( ( this->status == REMAP_PREPARE_START && normal > 0 ) ||
			( this->status == REMAP_PREPARE_END && remapping > 0 ) ||
			( this->status != REMAP_PREPARE_START && this->status != REMAP_PREPARE_END ) ) {
		pthread_rwlock_unlock( &this->stlock );
		return false;
	}

    switch ( this->status ) {
        case REMAP_PREPARE_START:
            signal = REMAP_WAIT_START;
            break;
        case REMAP_PREPARE_END:
            signal = REMAP_WAIT_END;
            break;
        default:
            pthread_rwlock_unlock( &this->stlock );
            return false;
    }

    len = sprintf( buf, "%d\n", signal );
    ret = SP_multicast( this->mbox, MSG_TYPE, COORD_GROUP, 0, len, buf );
    if ( ret == len ) {
        this->status = signal;
    }
    pthread_rwlock_unlock( &this->stlock );

    return true;
}
