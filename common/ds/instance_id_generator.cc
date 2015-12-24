#include "instance_id_generator.hh"

InstanceIdGenerator::InstanceIdGenerator() {
	LOCK_INIT( &this->lock );
	this->current = 0;
}

uint16_t InstanceIdGenerator::generate( Socket *socket ) {
	uint16_t ret;
	LOCK( &this->lock );
	do {
		this->current++;
		if ( this->current == 0 ) // Skip the reserved instance ID
			this->current++;
	} while ( this->mapping.find( this->current ) != this->mapping.end() );
	this->mapping[ this->current ] = socket;
	ret = this->current;
	UNLOCK( &this->lock );
	return ret;
}

bool InstanceIdGenerator::release( uint16_t instanceId ) {
	std::unordered_map<uint16_t, Socket *>::iterator it;
	bool ret;
	LOCK( &this->lock );
	it = this->mapping.find( instanceId );
	ret = ( it != this->mapping.end() );
	if ( ret )
		this->mapping.erase( it );
	UNLOCK( &this->lock );
	return ret;
}
