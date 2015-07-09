#ifndef __COMMON_DS_RING_BUFFER_HH__
#define __COMMON_DS_RING_BUFFER_HH__

#include <pthread.h>

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
	pthread_mutex_t mAccess;
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
		pthread_mutex_init(&mAccess, NULL);
		pthread_cond_init(&cvEmpty, NULL);
		pthread_cond_init(&cvFull, NULL);
	}

	~RingBuffer() {
		delete[] buffer;
	}

	inline int nextVal(int x) { return (x+1) >= max ? 0 : x+1; }
	// inline int nextVal(int x) { return (x+1)%max; }

	int Insert(T* data, int len) {
		pthread_mutex_lock(&mAccess);
		while (count == max) {
			pthread_cond_wait(&cvFull, &mAccess);
		}
        buffer[writeIndex].len = len;
		memcpy(&(buffer[writeIndex].data), data, len);
		writeIndex = nextVal(writeIndex);
		count++;
		pthread_cond_signal(&cvEmpty);
		pthread_mutex_unlock(&mAccess);
		return 0;
	}

	int Extract(T* data) {
		pthread_mutex_lock(&mAccess);
		while (count == 0) {
			if (!blockOnEmpty || !run) {
				pthread_mutex_unlock(&mAccess);
				return -1;
			}
			pthread_cond_wait(&cvEmpty, &mAccess);
		}
		memcpy(data, &(buffer[readIndex].data), buffer[readIndex].len);
		readIndex = nextVal(readIndex);
		count--;
		pthread_cond_signal(&cvFull);
		pthread_mutex_unlock(&mAccess);
		return 0;
	}

	int GetCount() {
		return count;
	}

	void Stop() {
		pthread_mutex_lock(&mAccess);
		run = false;
		while (count > 0) {
			pthread_cond_wait(&cvFull, &mAccess);
		}
		pthread_mutex_unlock(&mAccess);
		pthread_cond_broadcast(&cvEmpty);
	}
};

#endif 
