#ifndef __CLIENT_SOCKET_SERVER_SOCKET_HH__
#define __CLIENT_SOCKET_SERVER_SOCKET_HH__

#include <set>
#include "../../common/ds/array_map.hh"
#include "../../common/lock/lock.hh"
#include "../../common/socket/socket.hh"
#include "../../common/timestamp/timestamp.hh"
#include "../backup/backup.hh"

class ServerSocket : public Socket {
private:
	static ArrayMap<int, ServerSocket> *servers;

public:
	bool registered;
	Backup backup;
	struct {
		Timestamp current;
		Timestamp lastAck; // to server
		struct {
			std::multiset<Timestamp> _update;
			std::multiset<Timestamp> _del;
			LOCK_T updateLock;
			LOCK_T delLock;

			inline void insertUpdate( Timestamp timestamp ) {
				LOCK( &this->updateLock );
				this->_update.insert( timestamp );
				UNLOCK( &this->updateLock );
			}

			inline void insertDel( Timestamp timestamp ) {
				LOCK( &this->delLock );
				this->_del.insert( timestamp );
				UNLOCK( &this->delLock );
			}

			inline void eraseUpdate( uint32_t timestamp ) {
				std::multiset<Timestamp>::iterator it;
				LOCK( &this->updateLock );
				it = this->_update.find( timestamp );
				if ( it != this->_update.end() )
					this->_update.erase( it );
				UNLOCK( &this->updateLock );
			}

			inline void eraseDel( uint32_t timestamp ) {
				std::multiset<Timestamp>::iterator it;
				LOCK( &this->delLock );
				it = this->_del.find( timestamp );
				if ( it != this->_del.end() )
					this->_del.erase( it );
				UNLOCK( &this->delLock );
			}
		} pendingAck;
	} timestamp;
	LOCK_T ackParityDeltaBackupLock;
	uint16_t instanceId;

	static void setArrayMap( ArrayMap<int, ServerSocket> *servers );
	bool start();
	void stop();
	void registerClient();
	ssize_t send( char *buf, size_t ulen, bool &connected );
	ssize_t recv( char *buf, size_t ulen, bool &connected, bool wait );
	ssize_t recvRem( char *buf, size_t expected, char *prevBuf, size_t prevSize, bool &connected );
	bool done();
	bool ready();
	void print( FILE *f = stdout );
};

#endif
