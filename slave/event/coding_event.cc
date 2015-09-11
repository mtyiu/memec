#include "coding_event.hh"

void CodingEvent::decode( Chunk **chunks, BitmaskArray *status ) {
	this->type = CODING_EVENT_TYPE_DECODE;
	this->message.decode.chunks = chunks;
	this->message.decode.status = status;
}
