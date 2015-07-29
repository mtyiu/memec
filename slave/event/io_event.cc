#include "io_event.hh"

void IOEvent::flush( Chunk *chunk ) {
	this->type = IO_EVENT_TYPE_FLUSH_CHUNK;
	this->message.chunk = chunk;
}
