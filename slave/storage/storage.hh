#ifndef __SLAVE_STORAGE_STORAGE_HH__
#define __SLAVE_STORAGE_STORAGE_HH__

#include "storage_type.hh"
#include "../config/slave_config.hh"
#include "../../common/ds/chunk.hh"

class Storage {
public:
	static StorageType type;

	virtual ~Storage();
	virtual void init( SlaveConfig &config ) = 0;
	virtual bool start() = 0;
	virtual bool read( Chunk *chunk, uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isParity, long offset = 0, size_t length = 0 ) = 0;
	virtual ssize_t write( Chunk *chunk, bool sync, long offset = 0, size_t length = 0 ) = 0;
	virtual void sync() = 0;
	virtual void stop() = 0;

	static Storage *instantiate( SlaveConfig &config );
	static void destroy( Storage *storage );
};

#endif
