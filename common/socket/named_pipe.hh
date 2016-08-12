#ifndef __COMMON_SOCKET_NAMED_PIPE_HH__
#define __COMMON_SOCKET_NAMED_PIPE_HH__

#define NAMED_PIPE_PATHNAME_MAX_LENGTH 256
#define NAMED_PIPE_FILENAME_LENGTH 16

#include <unordered_map>

#include "../lock/lock.hh"

class NamedPipe {
private:
	LOCK_T lock;
	std::unordered_map<int, char *> pathnames;

	char *generate();

public:
	char *pathname;

	NamedPipe();
	void init( char *pathname );
	void getFullPath( char *dst, char *name );
	char *mkfifo();
	int open( char *name, bool readMode );
	bool close( int fd );
};

#endif
