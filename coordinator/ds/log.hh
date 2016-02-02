#ifndef __COORDINATOR_DS_LOG_HH__
#define __COORDINATOR_DS_LOG_HH__

enum LogType {
	LOG_TYPE_RECOVERY
};

class Log {
private:
	double timestamp;
	LogType type;
	union {
		struct {
			uint32_t addr;
			uint16_t port;
			uint32_t numChunks;
			uint32_t numUnsealedKeys;
			double elapsedTime;
		} recovery;
	} data;

public:
	void setTimestamp( double timestamp ) {
		this->timestamp = timestamp;
	}

	void setRecovery( uint32_t addr, uint16_t port, uint32_t numChunks, uint32_t numUnsealedKeys, double elapsedTime ) {
		this->type = LOG_TYPE_RECOVERY;
		this->data.recovery.addr = addr;
		this->data.recovery.port = port;
		this->data.recovery.numChunks = numChunks;
		this->data.recovery.numUnsealedKeys = numUnsealedKeys;
		this->data.recovery.elapsedTime = elapsedTime;
	}

	void print( FILE *f ) {
		fprintf( f, "%lf\t", this->timestamp );
		switch( this->type ) {
			case LOG_TYPE_RECOVERY:
				fprintf(
					f, "recovery\t%u\t%u\t%u\t%u\t%lf\n",
					this->data.recovery.addr,
					this->data.recovery.port,
					this->data.recovery.numChunks,
					this->data.recovery.numUnsealedKeys,
					this->data.recovery.elapsedTime
				);
				break;
			default:
				break;
		}
	}
};

#endif
