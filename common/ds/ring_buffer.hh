#ifndef __COMMON_DS_RING_BUFFER_HH__
#define __COMMON_DS_RING_BUFFER_HH__

#include "../lock/lock.hh"

template <class T> class RingBuffer {
	typedef struct {
		int len;
		T data;
	} /* __attribute((packed)) */ Buffer_t;
	Buffer_t* buffer;
	volatile int readIndex;
	volatile int writeIndex;
	volatile int count;				// current number of occupied elements
	volatile int max;				// capacity of the buffer
	volatile int run;
	volatile int blockOnEmpty;
	LOCK_T mAccess;
	pthread_cond_t cvEmpty;
	pthread_cond_t cvFull;

public:

	RingBuffer(int size, bool block=true) {
		if (size < 2) size = 2; // shouldn't allow value less than 2.
		buffer = new Buffer_t[size];
		//buffer = (Buffer_t*)malloc(sizeof(Buffer_t)*size);
		readIndex = 0;
		writeIndex = 0;
		count = 0;
		max = size;
		run = true;
		blockOnEmpty = block;
		LOCK_INIT(&mAccess, NULL);
		pthread_cond_init(&cvEmpty, NULL);
		pthread_cond_init(&cvFull, NULL);
	}

	~RingBuffer() {
		delete[] buffer;
	}

	inline int nextVal(int x) { return (x+1) >= max ? 0 : x+1; }
	// inline int nextVal(int x) { return (x+1)%max; }

	int Insert(T* data, int len) {
		LOCK(&mAccess);
		while (count == max) {
			pthread_cond_wait(&cvFull, &mAccess);
		}
		buffer[writeIndex].len = len;
		memcpy(&(buffer[writeIndex].data), data, len);
		writeIndex = nextVal(writeIndex);
		count++;
		pthread_cond_signal(&cvEmpty);
		UNLOCK(&mAccess);
		return 0;
	}

	int Extract(T* data) {
		LOCK(&mAccess);
		while (count == 0) {
			if (!blockOnEmpty || !run) {
				UNLOCK(&mAccess);
				return -1;
			}
			pthread_cond_wait(&cvEmpty, &mAccess);
		}
		memcpy(data, &(buffer[readIndex].data), buffer[readIndex].len);
		readIndex = nextVal(readIndex);
		count--;
		pthread_cond_signal(&cvFull);
		UNLOCK(&mAccess);
		return 0;
	}

	int GetCount() {
		return count;
	}

	void Stop() {
		LOCK(&mAccess);
		run = false;
		while (count > 0) {
			pthread_cond_wait(&cvFull, &mAccess);
		}
		UNLOCK(&mAccess);
		pthread_cond_broadcast(&cvEmpty);
	}
};

#endif
