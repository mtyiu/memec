#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "named_pipe.hh"

char *NamedPipe::generate() {
	int length = NAMED_PIPE_FILENAME_LENGTH;
	char *ret = ( char * ) malloc( NAMED_PIPE_FILENAME_LENGTH );
	char *tmp = ret;
	char charset[] = "0123456789"
					 "abcdefghijklmnopqrstuvwxyz"
					 "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	while ( length-- > 0 ) {
		size_t index = ( double ) rand() / RAND_MAX * ( sizeof( charset ) - 1 );
		*tmp++ = charset[ index ];
	}
	*tmp = '\0';
	return ret;
}

NamedPipe::NamedPipe() {
	LOCK_INIT( &this->lock );
}

void NamedPipe::init( char *pathname ) {
	this->pathname = pathname;
}

void NamedPipe::getFullPath( char *dst, char *name ) {
	sprintf( dst, "%s/%s", this->pathname, name );
}

char *NamedPipe::open( int &fd ) {
	char *ret = this->generate();
	char pathname[ NAMED_PIPE_PATHNAME_MAX_LENGTH ];
	this->getFullPath( pathname, ret );

	fd = mkfifo( pathname, 0600 );
	std::pair<int, char *> p( fd, ret );

	LOCK( &this->lock );
	this->pathnames.insert( p );
	UNLOCK( &this->lock );

	return ret;
}

bool NamedPipe::close( int fd ) {
	bool ret = false;
	std::unordered_map<int, char *>::iterator it;
	char pathname[ NAMED_PIPE_PATHNAME_MAX_LENGTH ];

	LOCK( &this->lock );
	it = this->pathnames.find( fd );
	if ( it == this->pathnames.end() ) {
		ret = false;
	} else {
		this->getFullPath( pathname, it->second );
		::unlink( pathname );
		::free( it->second );
		this->pathnames.erase( it );
		ret = true;
	}
	UNLOCK( &this->lock );

	return ret;
}
