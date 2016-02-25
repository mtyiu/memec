#ifndef __TEST_COMMON_REMAP_SIMPLE_REMAP_MSG_HANDLER_HH__
#define __TEST_COMMON_REMAP_SIMPLE_REMAP_MSG_HANDLER_HH__

#include <atomic>
#include "../../../common/remap/remap_group.hh"
#include "../../../common/remap/remap_msg_handler.hh"

class SimpleRemapMsgHandler : public RemapMsgHandler {
private:
	bool isMasterJoin( int service, char *msg, char *subject );
	bool isSlaveJoin( int service, char *msg, char *subject );
	
	static void *readMessages( void *argv );
public:
	SimpleRemapMsgHandler();
	SimpleRemapMsgHandler( char *group );
	~SimpleRemapMsgHandler();

	bool start();
	bool stop();
	bool join( const char* group );

	bool addAliveSlave( struct sockaddr_in slave );
	bool removeAliveSlave( struct sockaddr_in slave );

	bool sendStatePub ( std::vector<struct sockaddr_in> &slaves, int numGroup, const char targetGroup[][ MAX_GROUP_NAME ] );

	std::atomic<int> masters; 
	std::atomic<int> slaves;
};

#endif
