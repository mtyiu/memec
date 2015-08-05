#include <cerrno>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "local_storage.hh"
#include "../../common/util/debug.hh"

void LocalStorage::generatePath( uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isParity ) {
	snprintf(
		this->path + this->pathLength,
		STORAGE_PATH_MAX - this->pathLength,
		"/%u.%u.%s%u.chunk",
		listId,
		stripeId,
		isParity ? "p" : "",
		chunkId
	);
}

void LocalStorage::init( SlaveConfig &config ) {
	strcpy( this->path, config.storage.path );
	this->pathLength = strlen( this->path );
}

bool LocalStorage::start() {
	return true;
}

bool LocalStorage::read( Chunk *chunk, uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isParity, long offset, size_t length ) {
	int fd;
	ssize_t ret;
	struct stat st;

	this->generatePath( listId, stripeId, chunkId, isParity );

	// Error checking
	if ( ::stat( this->path, &st ) != 0 ) {
		__ERROR__( "LocalStorage", "read", "stat(): %s", strerror( errno ) );
		return false;
	}

	if ( ! S_ISREG( st.st_mode ) ) {
		__ERROR__( "LocalStorage", "read", "stat(): %s is not a regular file.", this->path );
		return false;
	}

	// Determine the number of bytes to be read
	offset = offset > 0 ? offset : 0;
	length = length == 0 ? st.st_size : ( st.st_size - offset < ( off_t ) length ? st.st_size - offset : length );

	fd = ::open( this->path, O_RDONLY );
	if ( fd == -1 ) {
		__ERROR__( "LocalStorage", "read", "open(): %s", strerror( errno ) );
		return false;
	}

	// Read data to chunk
	if ( offset > 0 ) {
		if ( lseek( fd, offset, SEEK_SET ) == -1 ) {
			__ERROR__( "LocalStorage", "read", "lseek(): %s", strerror( errno ) );
			return false;
		}
	}
	ret = ::read( fd, chunk->data + offset, length );
	if ( ret == -1 ) {
		__ERROR__( "LocalStorage", "read", "read(): %s", strerror( errno ) );
	} else if ( ret < ( ssize_t ) length ) {
		__ERROR__( "LocalStorage", "read", "read(): Number of bytes read is fewer than the specified size." );
		length = ret;
	}

	if ( ::close( fd ) == -1 ) {
		__ERROR__( "LocalStorage", "read", "close(): %s", strerror( errno ) );
	}

	chunk->status = CHUNK_STATUS_CACHED;
	chunk->metadata.listId = listId;
	chunk->metadata.stripeId = stripeId;
	chunk->metadata.chunkId = chunkId;
	chunk->isParity = isParity;
	if ( isParity )
		chunk->updateParity( offset, length );
	else
		chunk->updateData();

	return ret != -1;
}

ssize_t LocalStorage::write( Chunk *chunk, bool sync, long offset, size_t length ) {
	int fd;
	ssize_t ret;
	uint32_t size;

	this->generatePath(
		chunk->metadata.listId,
		chunk->metadata.stripeId,
		chunk->metadata.chunkId,
		chunk->isParity
	);

	// Determine the number of bytes to be written
	offset = offset > 0 ? offset : 0;
	size = chunk->size;
	chunk->size = Chunk::capacity; // Force the file size to be one complete chunk
	length = length == 0 ? chunk->size : ( chunk->size - offset < ( off_t ) length ? chunk->size - offset : length );
	chunk->size = size;

	fd = ::open( this->path, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR );
	if ( fd == -1 ) {
		__ERROR__( "LocalStorage", "write", "open(): %s", strerror( errno ) );
		return -1;
	}

	// Write data from chunk
	if ( offset > 0 ) {
		if ( lseek( fd, offset, SEEK_SET ) == -1 ) {
			__ERROR__( "LocalStorage", "write", "lseek(): %s", strerror( errno ) );
			return -1;
		}
	}
	ret = ::write( fd, chunk->data + offset, length );
	if ( ret == -1 ) {
		__ERROR__( "LocalStorage", "write", "write(): %s", strerror( errno ) );
	} else {
		if ( ret < ( ssize_t ) length ) {
			__ERROR__( "LocalStorage", "write", "write(): Number of bytes written is fewer than the specified size." );
			length = ret;
		}

		if ( sync && ::syncfs( fd ) == -1 ) {
			__ERROR__( "LocalStorage", "write", "syncfs(): %s", strerror( errno ) );
		}

		chunk->status = CHUNK_STATUS_CACHED;
	}

	if ( ::close( fd ) == -1 ) {
		__ERROR__( "LocalStorage", "write", "close(): %s", strerror( errno ) );
	}

	return ret;
}

void LocalStorage::sync() {
	::sync();
}

void LocalStorage::stop() {
	this->sync();
}
