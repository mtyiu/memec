#ifndef __COMMON_REMAP_REMAP_MSG_HANDLER_HH__
#define __COMMON_REMAP_REMAP_MSG_HANDLER_HH__

#include <cstdio>
#include <stdint.h>
#include <sys/types.h>
#include <pthread.h>
#include <sp.h>
#include "remap_status.hh"

#define MAX_MESSLEN     1024
#define MAX_SPREAD_NAME 1024
#define MAX_GROUP_NUM   2
#define GROUP_NAME      "plio"
#define MSG_TYPE        FIFO_MESS


class RemapMsgHandler {
protected:
    mailbox mbox;
    char privateGroup[ MAX_GROUP_NAME ];
    char spread[ MAX_SPREAD_NAME ];
    char user[ MAX_SPREAD_NAME ];
    char *group ;

    pthread_t reader;
    uint32_t msgCount;

    bool isConnected;
    RemapStatus status;

    inline void increMsgCount() {
        this->msgCount++;
    }

    inline void decreMsgCount() {
        this->msgCount--;
    }

    static inline bool isRegularMessage( int msgType ) {
        return ( msgType && REGULAR_MESS );
    }

    static inline bool isMemberJoin( int msgType ) {
        return ( msgType && CAUSED_BY_JOIN );
    }

    static inline bool isMemberLeave( int msgType ) {
        return ( ( msgType && CAUSED_BY_LEAVE ) || 
                ( msgType && CAUSED_BY_DISCONNECT ) );
    }

public:
    RemapMsgHandler() {
        this->reader = -1;
        this->isConnected = false;
        this->msgCount = 0;
        this->group = ( char* ) GROUP_NAME;
    }

    ~RemapMsgHandler() {} 
    
    inline bool getIsConnected () {
        return this->isConnected;
    }

    inline RemapStatus getStatus() {
        return this->status;
    }

    bool init( const char *spread = NULL, const char *user = NULL );
    void quit();
    
    virtual bool start() = 0;
    virtual bool stop() = 0;
};

#endif
