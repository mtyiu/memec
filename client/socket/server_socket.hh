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
			std::multiset< std::pair<Timestamp, uint32_t> > _update; // ts, application request id
			std::multiset< std::pair<Timestamp, uint32_t> > _del; // ts, application request id
			LOCK_T updateLock;
			LOCK_T delLock;

			inline void insertUpdate( Timestamp timestamp, uint32_t id ) {
				std::pair<Timestamp, uint32_t> record ( timestamp, id );
				LOCK( &this->updateLock );
				this->_update.insert( record );
				UNLOCK( &this->updateLock );
			}

			inline void insertDel( Timestamp timestamp, uint32_t id ) {
				std::pair<Timestamp, uint32_t> record ( timestamp, id );
				LOCK( &this->delLock );
				this->_del.insert( record );
				UNLOCK( &this->delLock );
			}

			inline void eraseUpdate( uint32_t timestamp, uint32_t id ) {
				std::pair<Timestamp, uint32_t> record ( timestamp, id );
				std::multiset< std::pair<Timestamp, uint32_t> >::iterator it;
				LOCK( &this->updateLock );
				it = this->_update.find( record );
				if ( it != this->_update.end() )
					this->_update.erase( it );
				UNLOCK( &this->updateLock );
			}

			inline void eraseDel( uint32_t timestamp, uint32_t id ) {
				std::pair<Timestamp, uint32_t> record ( timestamp, id );
				std::multiset< std::pair<Timestamp, uint32_t> >::iterator it;
				LOCK( &this->delLock );
				it = this->_del.find( record );
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
