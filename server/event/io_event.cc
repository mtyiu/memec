#include "io_event.hh"

void IOEvent::flush( Chunk *chunk, bool clear ) {
	this->type = IO_EVENT_TYPE_FLUSH_CHUNK;
	this->chunk = chunk;
	this->clear = clear;
}
