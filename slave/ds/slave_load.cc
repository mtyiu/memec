#include "slave_load.hh"

SlaveLoad::SlaveLoad() {}

void SlaveLoad::reset() {
	this->elapsedTime = 0.0;
	this->sentBytes = 0;
	this->recvBytes = 0;
	Load::reset();
}

void SlaveLoad::aggregate( SlaveLoad &l ) {
	if ( l.elapsedTime > this->elapsedTime )
		this->elapsedTime = l.elapsedTime;
	this->sentBytes += l.sentBytes;
	this->recvBytes += l.recvBytes;
	Load::aggregate( l );
}

void SlaveLoad::print( FILE *f ) {
	int width = 27;
	fprintf( f,
		"%-*s : %-10.4lf s\n"
		"%-*s : %lu bytes\n"
		"%-*s : %lu bytes\n",
		// "%-*s : %-6.2lf MBps\n"
		// "%-*s : %-6.2lf MBps\n",
		width, "Elapsed time", this->elapsedTime,
		width, "Number of bytes sent", this->sentBytes,
		width, "Number of bytes received", this->recvBytes
		// width, "Average send throughput", ( ( double ) this->sentBytes / this->elapsedTime ) / ( 1024 * 1024 ),
		// width, "Average receive throughput", ( ( double ) this->recvBytes / this->elapsedTime ) / ( 1024 * 1024 )
	);
	Load::print( f );
}

void SlaveLoad::updateChunk() {
	this->ops.update++;
}

void SlaveLoad::delChunk() {
	this->ops.del++;
}

void SlaveLoad::getChunk() {
	this->ops.get++;
}

void SlaveLoad::setChunk() {
	this->ops.set++;
}
