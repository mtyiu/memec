#ifndef __SLAVE_DS_SLAVE_LOAD_HH__
#define __SLAVE_DS_SLAVE_LOAD_HH__

#include "../../common/ds/load.hh"

class SlaveLoad : public Load {
public:
	uint64_t _sentBytes;
	uint64_t _recvBytes;

	uint32_t _sealChunk;
	uint32_t _getChunk;
	uint32_t _setChunk;

	SlaveLoad();
	void reset();
	void aggregate( SlaveLoad &l );
	void print( FILE *f = stdout );

	inline void sentBytes( uint64_t _sentBytes ) { this->_sentBytes += _sentBytes; }
	inline void recvBytes( uint64_t _recvBytes ) { this->_recvBytes += _recvBytes; }
	inline void sealChunk() { this->_sealChunk++; }
	inline void updateChunk() { this->_update++; }
	inline void delChunk() { this->_del++; }
	inline void getChunk() { this->_getChunk++; }
	inline void setChunk() { this->_setChunk++; }
};

#endif
