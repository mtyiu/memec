#ifndef __TEST_COMMON_REMAP_SIMPLE_REMAP_MSG_HANDLER_HH__
#define __TEST_COMMON_REMAP_SIMPLE_REMAP_MSG_HANDLER_HH__

#include <atomic>
#include "../../../common/remap/remap_group.hh"
#include "../../../common/remap/remap_msg_handler.hh"

class SimpleRemapMsgHandler : public RemapMsgHandler {
private:
	bool isClientJoin( int service, char *msg, char *subject );
	bool isServerJoin( int service, char *msg, char *subject );

	static void *readMessages( void *argv );
public:
	SimpleRemapMsgHandler();
	SimpleRemapMsgHandler( char *group );
	~SimpleRemapMsgHandler();

	bool start();
	bool stop();
	bool join( const char* group );

	bool addAliveServer( struct sockaddr_in server );
	bool removeAliveServer( struct sockaddr_in server );

	int sendStatePub ( std::vector<struct sockaddr_in> &servers, int numGroup, const char targetGroup[][ MAX_GROUP_NAME ] );

	std::atomic<int> clients;
	std::atomic<int> servers;
};

#endif
