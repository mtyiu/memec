#ifndef __SLAVE_STORAGE_LOCAL_STORAGE_HH__
#define __SLAVE_STORAGE_LOCAL_STORAGE_HH__

#include "storage.hh"

class LocalStorage : public Storage {
private:
	size_t pathLength;
	char path[ STORAGE_PATH_MAX ];

	void generatePath( uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isParity );

public:
	void init( SlaveConfig &config );
	bool start();
	bool read( Chunk *chunk, uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isParity, long offset = 0, size_t length = 0 );
	ssize_t write( Chunk *chunk, bool sync, long offset = 0, size_t length = 0 );
	void sync();
	void stop();
};

#endif
