#ifndef __COMMON_DS_INSTANCE_ID_GENERATOR_HH__
#define __COMMON_DS_INSTANCE_ID_GENERATOR_HH__

#include <unordered_map>
#include "../../common/lock/lock.hh"
#include "../../common/socket/socket.hh"

// Implement the singleton pattern
class InstanceIdGenerator {
private:
	LOCK_T lock;
	uint16_t current;
	std::unordered_map<uint16_t, Socket *> mapping;

	InstanceIdGenerator();
	// Do not implement
	InstanceIdGenerator( InstanceIdGenerator const& );
	void operator=( InstanceIdGenerator const& );

public:
	static InstanceIdGenerator *getInstance() {
		static InstanceIdGenerator instanceIdGenerator;
		return &instanceIdGenerator;
	}

	uint16_t generate( Socket *socket );
	bool release( uint16_t instanceId );
};

#endif
