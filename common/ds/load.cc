#include "load.hh"

Load::Load() {
	this->reset();
}

void Load::reset() {
	this->_get = 0;
	this->_set = 0;
	this->_update = 0;
	this->_del = 0;
}

void Load::aggregate( Load &l ) {
	this->_get += l._get;
	this->_set += l._set;
	this->_update += l._update;
	this->_del += l._del;
}

void Load::print( FILE *f ) {
	int width = 27;
	fprintf( f,
		"%-*s : %u\n"
		"%-*s : %u\n"
		"%-*s : %u\n"
		"%-*s : %u\n",
		width, "Number of GET operations", this->_get,
		width, "Number of SET operations", this->_set,
		width, "Number of UPDATE operations", this->_update,
		width, "Number of DELETE operations", this->_del
	);
}
