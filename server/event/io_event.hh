#ifndef __SERVER_EVENT_IO_EVENT_HH__
#define __SERVER_EVENT_IO_EVENT_HH__

#include "../../common/ds/chunk.hh"
#include "../../common/event/event.hh"

enum IOEventType {
	IO_EVENT_TYPE_FLUSH_CHUNK
};

class IOEvent : public Event<void> {
public:
	IOEventType type;
	Chunk *chunk;

	inline void flush( Chunk *chunk ) {
		this->type = IO_EVENT_TYPE_FLUSH_CHUNK;
		this->chunk = chunk;
	}
};

#endif
