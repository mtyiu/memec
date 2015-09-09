#include <cstring>
#include <cstdlib>
#include <arpa/inet.h>
#include <sp.h>
#include <unistd.h>
#include "remap_msg_handler.hh"
#include "../../common/remap/remap_group.hh"

using namespace std;

#define POLL_ACK_TIME_INTVL     1   // in seconds

CoordinatorRemapMsgHandler::CoordinatorRemapMsgHandler() : 
        RemapMsgHandler() {
    this->group = ( char* ) COORD_GROUP;
    pthread_rwlock_init( &this->mastersLock, NULL );
}

CoordinatorRemapMsgHandler::~CoordinatorRemapMsgHandler() {
}

bool CoordinatorRemapMsgHandler::init( const int ip, const int port, const char *user ) {
    char addrbuf[ 32 ], ipstr[ INET_ADDRSTRLEN ];
    struct in_addr addr;
    memset( addrbuf, 0, 32 );
    addr.s_addr = ip;
    inet_ntop( AF_INET, &addr, ipstr, INET_ADDRSTRLEN );
    sprintf( addrbuf, "%u@%s", port, ipstr );
    RemapMsgHandler::init( addrbuf , user );

    this->isListening = false;
    return ( SP_join( this->mbox, MASTER_GROUP ) == 0 );
}

void CoordinatorRemapMsgHandler::quit() {
    SP_leave( mbox, COORD_GROUP );
    RemapMsgHandler::quit();
    if ( reader > 0 ) {
        pthread_join( this->reader, NULL );
        reader = -1;
    }
}

bool CoordinatorRemapMsgHandler::start() {
    if ( ! this->isConnected )
        return false;

    // read messages using a background thread
    if ( pthread_create( &this->reader, NULL, CoordinatorRemapMsgHandler::readMessages, this ) < 0 ) {
        fprintf( stderr, "Coordinator FAILED to start reading messages" );
        return false;
    }
    this->isListening = true;
    return true;
}

bool CoordinatorRemapMsgHandler::stop() {
    int ret = 0;
    if ( ! this->isConnected || ! this->isListening )
        return false;
    // no longer listen to incoming messages
    this->isListening = false;
    // avoid blocking call from blocking the stop action
    ret = pthread_cancel( this->reader );

    return ( ret == 0 );
}

bool CoordinatorRemapMsgHandler::startRemap() {
    pthread_rwlock_wrlock( &this->stlock );
    if ( this->status != REMAP_NONE && 
            this->status != REMAP_PREPARE_START ) {
        pthread_rwlock_unlock( &this->stlock );
        return false;
    }

    resetMasterAck();

    // ask master to prepare for start
    if ( this->sendMessageToMasters( REMAP_PREPARE_START ) == false ) {
        pthread_rwlock_unlock( &this->stlock );
        return false;
    }
    this->status = REMAP_PREPARE_START;
    pthread_rwlock_unlock( &this->stlock );

    // wait for ack (busy waiting)
    while( isAllMasterAcked() == false )
        sleep(POLL_ACK_TIME_INTVL);
        
    // ask master to start remap
    pthread_rwlock_wrlock( &this->stlock );
    if ( this->sendMessageToMasters( REMAP_START ) == false ) {
        pthread_rwlock_unlock( &this->stlock );
        return false;
    }
    this->status = REMAP_START;
    pthread_rwlock_unlock( &this->stlock );
    return true;
}

bool CoordinatorRemapMsgHandler::stopRemap() {

    pthread_rwlock_wrlock( &this->stlock );
    if ( this->status != REMAP_START &&
            this->status != REMAP_PREPARE_END ) {
        pthread_rwlock_unlock( &this->stlock );
        return false;
    }

    resetMasterAck();

    // ask master to stop remapping
    if ( this->sendMessageToMasters( REMAP_PREPARE_END ) == false ) {
        pthread_rwlock_unlock( &this->stlock );
        return false;
    }
    this->status = REMAP_PREPARE_END ;
    pthread_rwlock_unlock( &this->stlock );

    // TODO wait for ack.
    while( isAllMasterAcked() == false )
        sleep(1);

    // ask master to use normal SET workflow 
    pthread_rwlock_wrlock( &this->stlock );
    if ( this->sendMessageToMasters( REMAP_END ) == false ) {
        pthread_rwlock_unlock( &this->stlock );
        return false;
    }
    this->status = REMAP_NONE ;
    pthread_rwlock_unlock( &this->stlock );
    return true;
}

bool CoordinatorRemapMsgHandler::isMasterLeft( int service, char *msg, char *subject ) {
    // assume masters name themselves "master[0-9]*"
    if ( this->isMemberLeave( service ) ) {
        if ( strncmp( subject + 1, MASTER_PREFIX , MASTER_PREFIX_LEN ) == 0 ) {
            return true;
        }
    }
    return false;
}

bool CoordinatorRemapMsgHandler::isMasterJoin( int service, char *msg, char *subject ) {
    // assume masters name themselves "master[0-9]*"
    if ( this->isMemberJoin( service ) ) {
        if ( strncmp( subject + 1, MASTER_PREFIX , MASTER_PREFIX_LEN ) == 0 ) {
            return true;
        } else {
        }
    }
    return false;
}

bool CoordinatorRemapMsgHandler::sendMessageToMasters( RemapStatus to ) {
    if ( to == REMAP_UNDEFINED ) {
        to = this->status;
    }
    char buf[ MAX_MESSLEN ];
    int len = sprintf( buf, "%d\n", to );
    return ( SP_multicast ( this->mbox, MSG_TYPE, MASTER_GROUP, 0, len, buf ) > 0 ); 
            
}

void *CoordinatorRemapMsgHandler::readMessages( void *argv ) {
    int ret = 0;
    
    int service, groups, endian;
    int16 msgType;
    char sender[ MAX_GROUP_NAME ], msg[ MAX_MESSLEN ];
    char targetGroups[ MAX_GROUP_NUM ][ MAX_GROUP_NAME ];
    char* subject;

    bool regular = false, fromMaster = false;

    CoordinatorRemapMsgHandler *myself = ( CoordinatorRemapMsgHandler* ) argv;

    while( myself->isListening && ret >= 0 ) {
        ret = SP_receive( myself->mbox, &service, sender, MAX_GROUP_NUM, &groups, targetGroups, &msgType, &endian, MAX_MESSLEN, msg );

        subject = &msg[ SP_get_vs_set_offset_memb_mess() ];
        regular = myself->isRegularMessage( service );
        fromMaster = ( strncmp( sender, MASTER_GROUP, MASTER_GROUP_LEN ) == 0 );

        if ( ! regular && fromMaster && myself->isMasterJoin( service , msg, subject ) ) {
            // master joined ( masters group )
            myself->addAliveMaster( subject );
            pthread_rwlock_rdlock( &myself->stlock );
            // notify the new master about the remapping status
            myself->sendMessageToMasters();
            pthread_rwlock_unlock( &myself->stlock );
        } else if ( ! regular && myself->isMasterLeft( service , msg, subject ) ) {
            // master left
            myself->removeAliveMaster( subject );
        } else if ( regular ){
            // ack from masters, etc.
            myself->updateStatus( sender, msg, ret );
        }

        myself->increMsgCount();
    }
    if ( ret < 0 )
        fprintf( stderr, "coord reader exits with error code %d\n", ret );

    return ( void* ) &myself->msgCount;
}

bool CoordinatorRemapMsgHandler::updateStatus( char *subject, char *msg, int len ) {

    RemapStatus reply = ( RemapStatus ) atoi ( msg );

    // ignore messages that not from masters
    if ( strncmp( subject + 1, MASTER_PREFIX, MASTER_PREFIX_LEN ) != 0 ) {
        return false;
    }

    pthread_rwlock_rdlock( &this->stlock );
    if ( ( this->status == REMAP_PREPARE_START && reply == REMAP_WAIT_START ) || 
            ( this->status == REMAP_PREPARE_END && reply == REMAP_WAIT_END ) ) {
        // ack to preparation for starting/ending the remapping phase
        pthread_rwlock_wrlock( &this->mastersLock );
        if ( aliveMasters.count( string( subject ) ) )
             ackMasters.insert( string( subject ) );
        //else
        //    fprintf( stderr, "master not found %s!!\n", subject );
        pthread_rwlock_unlock( &this->mastersLock );
    } 
    pthread_rwlock_unlock( &this->stlock );
    
    return true;
}

void CoordinatorRemapMsgHandler::addAliveMaster( char *name ) {
    pthread_rwlock_wrlock( &this->mastersLock );
    aliveMasters.insert( string( name ) );
    pthread_rwlock_unlock( &this->mastersLock );
}

void CoordinatorRemapMsgHandler::removeAliveMaster( char *name ) {
    pthread_rwlock_wrlock( &this->mastersLock );
    aliveMasters.erase( string( name ) );
    ackMasters.erase( string( name ) );
    pthread_rwlock_unlock( &this->mastersLock );
}

bool CoordinatorRemapMsgHandler::resetMasterAck() {
    pthread_rwlock_wrlock( &this->mastersLock );
    ackMasters.clear();
    pthread_rwlock_unlock( &this->mastersLock );
    return true;
}

bool CoordinatorRemapMsgHandler::isAllMasterAcked() {
    bool allAcked = false;
    pthread_rwlock_wrlock( &this->mastersLock );
    allAcked = ( aliveMasters.size() == ackMasters.size() );
    fprintf( stderr, "%lu of %lu masters acked\n", ackMasters.size(), aliveMasters.size() );
    pthread_rwlock_unlock( &this->mastersLock );
    return allAcked;
}
