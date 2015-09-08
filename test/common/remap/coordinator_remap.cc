#include <unistd.h>
#include "../../../common/remap/remap_status.hh"
#include "../../../coordinator/remap/remap_msg_handler.hh"

#define TIME_OUT 2
#define JOIN_TIME_OUT 10

int main () {
    CoordinatorRemapMsgHandler *ch = new CoordinatorRemapMsgHandler();
    
    fprintf( stderr, "START testing coordinator remapping message handler\n" );
    ch->init( COORD_PREFIX );

    if ( ! ch->start() ) {
        fprintf( stderr, "!! Cannot start reading message with message handler !!\n" );
    } else {
        fprintf( stderr, ".. wait for masters to join in %d seconds\n", JOIN_TIME_OUT );
        sleep( JOIN_TIME_OUT );
        fprintf( stderr, ".. Start remapping phase\n" );
        ch->startRemap();
        while( ch->getStatus() != REMAP_START ) 
            sleep( TIME_OUT );
        fprintf( stderr, ".. Stop remapping phase\n" );
        ch->stopRemap();
        while( ch->getStatus() != REMAP_NONE ) 
            sleep( TIME_OUT );
        ch->stop();
    }
    ch->quit();
    delete ch;

    fprintf( stderr, "END testing coordinator remapping message handler\n" );

    return 0;
}
