#include "load.hh"

Load::Load() {
	this->reset();
}

void Load::reset() {
	this->ops.get = 0;
	this->ops.set = 0;
	this->ops.update = 0;
	this->ops.del = 0;
}

void Load::aggregate( Load &l ) {
	this->ops.get += l.ops.get;
	this->ops.set += l.ops.set;
	this->ops.update += l.ops.update;
	this->ops.del += l.ops.del;
}

void Load::print( FILE *f ) {
	int width = 27;
	fprintf( f,
		"%-*s : %u\n"
		"%-*s : %u\n"
		"%-*s : %u\n"
		"%-*s : %u\n",
		width, "Number of GET operations", this->ops.get,
		width, "Number of SET operations", this->ops.set,
		width, "Number of UPDATE operations", this->ops.update,
		width, "Number of DELETE operations", this->ops.del
	);
}
