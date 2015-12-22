#ifndef __SLAVE_ACK_PENDING_ACK_HH__
#define __SLAVE_ACK_PENDING_ACK_HH__

#include <unordered_set>
#include <cstdint>
#include "../../common/lock/lock.hh"

class PendingAck {
private:
	LOCK_T lock;
	std::vector<uint32_t> timestamps;
	uint32_t lastAckTimestamp;

public:
	PendingAck() {
		LOCK_INIT( &this->lock );
		this->lastAckTimestamp = 0;
	}

	void insert( uint32_t timestamp ) {
		LOCK( &this->lock );
		this->timestamps.push_back( timestamp );
		UNLOCK( &this->lock );
	}

	bool erase( uint32_t to, uint32_t &from ) {
		bool ret = false;
		LOCK( &this->lock );
		for ( size_t i = 0, size = this->timestamps.size(); i < size; i++ ) {
			if ( this->timestamps[ i ] == to ) {
				ret = true;
				from = this->lastAckTimestamp;
				this->timestamps.erase( this->timestamps.begin() + i );
				this->lastAckTimestamp = to;
				break;
			}
		}
		UNLOCK( &this->lock );
		if ( ret )
			printf( "PendingAck::erase(): (%u, %u]\n", from, to );
		return ret;
	}
};

#endif
