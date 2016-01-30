#ifndef __COMMON_DS_PENDING_HH__
#define __COMMON_DS_PENDING_HH__

#include <map>
#include <cstring>
#include "../../common/ds/key.hh"
#include "../../common/hash/hash_func.hh"

class KeyValueUpdate : public Key {
public:
	uint32_t offset, length;
	bool isDegraded;
};

class PendingIdentifier {
	// TODO: May have problem when there are multiple clients
public:
	uint16_t instanceId, parentInstanceId;
	uint32_t requestId, parentRequestId;
	uint32_t timestamp;
	void *ptr;

	PendingIdentifier() {
		this->set( 0, 0, 0, 0, 0, 0 );
	}

	PendingIdentifier( uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr ) {
		this->set( instanceId, parentInstanceId, requestId, parentRequestId, 0, ptr );
	}

	PendingIdentifier( uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, uint32_t timestamp, void *ptr ) {
		this->set( instanceId, parentInstanceId, requestId, parentRequestId, timestamp, ptr );
	}

	void set( uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, uint32_t timestamp, void *ptr ) {
		this->instanceId = instanceId;
		this->parentInstanceId = parentInstanceId;
		this->requestId = requestId;
		this->parentRequestId = parentRequestId;
		this->timestamp = timestamp;
		this->ptr = ptr;
	}

	bool operator<( const PendingIdentifier &p ) const {
		if ( this->instanceId < p.instanceId )
			return true;
		if ( this->instanceId > p.instanceId )
			return false;

		if ( this->requestId < p.requestId )
			return true;
		if ( this->requestId > p.requestId )
			return false;

		return this->ptr < p.ptr;
	}

	bool operator==( const PendingIdentifier &p ) const {
		return ( this->instanceId == p.instanceId && this->requestId == p.requestId );
	}
};

namespace std {
	template<> struct hash<PendingIdentifier> {
		size_t operator()( const PendingIdentifier &pid ) const {
			size_t ret = 0;
			ret |= ( size_t ) pid.requestId;
			ret |= ( ( ( size_t ) pid.instanceId ) << 32 );
			return ret;
		}
	};
}

#endif
