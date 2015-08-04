#ifndef __MASTER_PROTOCOL_PROTOCOL_HH__
#define __MASTER_PROTOCOL_PROTOCOL_HH__

#include "../../common/ds/bitmask_array.hh"
#include "../../common/protocol/protocol.hh"

class MasterProtocol : public Protocol {
public:
	BitmaskArray *status; // Indicate which slave in the stripe is accessing the internal buffer

	MasterProtocol() : Protocol( ROLE_MASTER ) {}
	bool init( size_t size, uint32_t parityChunkCount );
	void free();

	// Coordinator
	char *reqRegisterCoordinator( size_t &size );
	// Slave
	char *reqRegisterSlave( size_t &size );
	char *reqSet( size_t &size, char *key, uint8_t keySize, char *value, uint32_t valueSize );
	char *reqGet( size_t &size, char *key, uint8_t keySize );
	char *reqUpdate( size_t &size, char *key, uint8_t keySize, char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize );
	char *reqDelete( size_t &size, char *key, uint8_t keySize );
	// Application
	char *resRegisterApplication( size_t &size, bool success );
	char *resSet( size_t &size, bool success, uint8_t keySize, char *key );
	char *resGet( size_t &size, bool success, uint8_t keySize, char *key, uint32_t valueSize = 0, char *value = 0 );
	char *resUpdate( size_t &size, bool success, uint8_t keySize, char *key );
	char *resDelete( size_t &size, bool success, uint8_t keySize, char *key );
};

#endif
