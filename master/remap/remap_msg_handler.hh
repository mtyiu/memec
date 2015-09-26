#ifndef __MASTER_REMAP_REMAP_MSG_HANDLER_HH__
#define __MASTER_REMAP_REMAP_MSG_HANDLER_HH__

#include "../../common/remap/remap_msg_handler.hh"
#include "../../common/remap/remap_group.hh"

class MasterRemapMsgHandler : public RemapMsgHandler {
private:
    bool isListening;

    void setStatus( char* msg, int len );
    static void *readMessages( void *argv );

public:

    MasterRemapMsgHandler();
    ~MasterRemapMsgHandler();

    bool init( const int ip, const int port, const char *user = NULL );
    void quit();

    bool start();
    bool stop();

	bool useRemappingFlow();
	bool allowRemapping();
    bool ackRemap( uint32_t normal = 0, uint32_t remapping = 0 );
};

#endif
