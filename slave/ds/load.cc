#include "load.hh"

Load::Load() {
	this->reset();
}

void Load::reset() {
	this->elapsedTime = 0.0;
	this->sentBytes = 0;
	this->recvBytes = 0;
	this->ops.get = 0;
	this->ops.set = 0;
	this->ops.update = 0;
	this->ops.del = 0;
}

void Load::aggregate( Load &l ) {
	if ( l.elapsedTime > this->elapsedTime )
		this->elapsedTime = l.elapsedTime;

	this->sentBytes += l.sentBytes;
	this->recvBytes += l.recvBytes;
	this->ops.get += l.ops.get;
	this->ops.set += l.ops.set;
	this->ops.update += l.ops.update;
	this->ops.del += l.ops.del;
}

void Load::print( FILE *f ) {
	int width = 27;
	fprintf( f,
		"%-*s : %-10.4lf s\n"
		"%-*s : %lu bytes\n"
		"%-*s : %lu bytes\n"
		// "%-*s : %-6.2lf MBps\n"
		// "%-*s : %-6.2lf MBps\n"
		"%-*s : %u\n"
		"%-*s : %u\n"
		"%-*s : %u\n"
		"%-*s : %u\n",
		width, "Elapsed time", this->elapsedTime,
		width, "Number of bytes sent", this->sentBytes,
		width, "Number of bytes received", this->recvBytes,
		// width, "Average send throughput", ( ( double ) this->sentBytes / this->elapsedTime ) / ( 1024 * 1024 ),
		// width, "Average receive throughput", ( ( double ) this->recvBytes / this->elapsedTime ) / ( 1024 * 1024 ),
		width, "Number of GET operations", this->ops.get,
		width, "Number of SET operations", this->ops.set,
		width, "Number of UPDATE operations", this->ops.update,
		width, "Number of DELETE operations", this->ops.del
	);
}
