#ifndef __MASTER_PROTOCOL_PROTOCOL_HH__
#define __MASTER_PROTOCOL_PROTOCOL_HH__

#include "../../common/ds/bitmask_array.hh"
#include "../../common/protocol/protocol.hh"

class MasterProtocol : public Protocol {
public:
	volatile bool *status; // Indicate which slave in the stripe is accessing the internal buffer

	MasterProtocol() : Protocol( ROLE_MASTER ) {}
	bool init( size_t size, uint32_t parityChunkCount );
	void free();

	/* Coordinator */
	// Register
	char *reqRegisterCoordinator( size_t &size );

	/* Slave */
	// Register
	char *reqRegisterSlave( size_t &size );
	// SET
	char *reqSet( size_t &size, char *key, uint8_t keySize, char *value, uint32_t valueSize );
	// GET
	char *reqGet( size_t &size, char *key, uint8_t keySize );
	// UPDATE
	char *reqUpdate( size_t &size, char *key, uint8_t keySize, char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize );
	// DELETE
	char *reqDelete( size_t &size, char *key, uint8_t keySize );

	/* Application */
	// Register
	char *resRegisterApplication( size_t &size, bool success );
	// SET
	char *resSet( size_t &size, bool success, uint8_t keySize, char *key );
	// GET
	char *resGet( size_t &size, bool success, uint8_t keySize, char *key, uint32_t valueSize = 0, char *value = 0 );
	// UPDATE
	char *resUpdate( size_t &size, bool success, uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize );
	// DELETE
	char *resDelete( size_t &size, bool success, uint8_t keySize, char *key );
};

#endif
