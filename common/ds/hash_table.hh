#ifndef __COMMON_DS_HASH_TABLE_HH__
#define __COMMON_DS_HASH_TABLE_HH__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define NUM_ROW_FACTOR   7
#define GLINK_PERIOD	 0

/***************************************************************************
 * Definition of the HashTable template
 ***************************************************************************/
template<class T> 
class HashTable {
private:
	// type definition
	typedef struct hash_node_s {
		T entry;						// key-value entry
		int index;						// index of the bucket
		double ts;						// timestamp of being referenced
		struct hash_node_s* prev;		// row node list
		struct hash_node_s* next;		
		struct hash_node_s* gprev;		// global node list
		struct hash_node_s* gnext; 
	} HASH_NODE;
	typedef unsigned int (* HASH_FCN)(const T*);
	typedef bool (* CMP_FCN)(const T*, const T*);
	typedef void (* INIT_FCN)(T*, void*);
	typedef void (* FREE_FCN)(T*, void*);

	
	// variables
	char name_[20];				// name of the hash table
	int num_row_;				// number of rows in the hash table
	int maxnum_;				// maximum number of nodes in the table
	double expire_timeout_;		// expire timeout value
	HASH_FCN hash_fcn_;			// hash function
	CMP_FCN cmp_fcn_;			// compare function
	INIT_FCN init_fcn_;			// function to initialize an entry
	FREE_FCN free_fcn_;			// function to cleanup an entry
	void* st_;					// state table object that initializes the
								// hash table
	HASH_NODE** table_;			// array of hash node ptrs (row lists)
	HASH_NODE* ghead_, *gtail_; // global list - all allocated nodes
	HASH_NODE* fptr_;			// ptr to the available free node

	// private static member functions
	static int is_prime(int);
	static int calc_next_prime(int);

	// private instance member functions
	void link_node(HASH_NODE *);
	void unlink_node(HASH_NODE *);
	void glink_node(HASH_NODE *);
	void gunlink_node(HASH_NODE *);
	HASH_NODE * get_free_node();
	HASH_NODE * find_node_row(const T*, int*);
	void expire(double);

public:
	// function prototypes
	HashTable(const char*, uint32_t, double, 
			HASH_FCN, CMP_FCN, INIT_FCN, FREE_FCN, void*);
	T* find(const T*, double, bool);
	bool is_full();
	void reset();
};

/***************************************************************************
 * Implementation of the HashTable template (note: template implementation
 * must be in the .hh file)
 ***************************************************************************/

/*
 * private static member functions
 */
/*
 * Check if num is a prime number
 */
template<class T>
int HashTable<T>::is_prime(int num) {
	int i;
	for (i=2; i<num; i++) {
		if ((num % i) == 0) {
			break;
		}
	}
	if (i == num) {
		return 1;
	}
	return 0;
}

/*
 * Find the next prime number after num
 */
template<class T>
int HashTable<T>::calc_next_prime(int num) {
	while (!is_prime(num)) {
		num++;
	}
	return num;
}

/*
 * private instance member functions
 */

/*
 * Link the hash node to the front of the row list
 */
template<class T> 
void HashTable<T>::link_node(HASH_NODE* h) {
	int index = h->index;
	if (index == -1) {			// unexpected error
		fprintf(stderr, "index in link_node() == -1 (%s)", name_);
		return;
	}
	if (table_[index] != 0) {	// add node to the front of the row list
		h->prev = 0;
		h->next = table_[index];
		table_[index]->prev = h;
		table_[index] = h;
	} else {					// 1st node in the row list
		h->prev = 0;
		h->next = 0;
		table_[index] = h;
	}
}

/*
 * Unlink the hash node from the row list
 */
template<class T> 
void HashTable<T>::unlink_node(HASH_NODE* h) {
	int index = h->index;
	if (index == -1) {			// unexpected error
		fprintf(stderr, "index in unlink_node() == -1 (%s)", name_);
		return;
	}
	if (h->prev != 0) {				// h is not the first node
		h->prev->next = h->next;
	} else {						// h is the first node
		table_[index] = h->next;
	}
	if (h->next != 0) {			// h is not the last node
		h->next->prev = h->prev;
	} 
	// note: either link_node() should be called to reset the row list
	// pointers, or the row list pointers are properly reset
}

/*
 * Link the hash node to the front of the global list
 */
template<class T> 
void HashTable<T>::glink_node(HASH_NODE* h) {
	// ghead_ should always be non-null after gunlink_node is called as it has
	// more than 1 node
	h->gprev = 0;
	h->gnext = ghead_;
	ghead_->gprev = h;
	ghead_ = h;
}

/*
 * Unlink the hash node from the global list
 */
template<class T> 
void HashTable<T>::gunlink_node(HASH_NODE* h) {
	if (h->gprev != 0) {			// h is not the first node
		h->gprev->gnext = h->gnext;
	} else {						// h is the first node
		ghead_ = h->gnext;
	}
	if (h->gnext != 0) {			// h is not the last node
		h->gnext->gprev = h->gprev;
	} else {
		gtail_ = h->gprev;
	}
	// note: glink_node() should be called to reset
	// the global list pointers
}

/*
 * Get a free node from the free pointer
 */
template<class T> 
typename HashTable<T>::HASH_NODE * 
HashTable<T>::get_free_node() {
	HASH_NODE * h = fptr_;

	if (h != NULL) {
		fptr_ = fptr_->gnext;
	} else {				// the global list has been used up
		// RECLAIM  
		
		// get the oldest (last) node
		h = gtail_;
		free_fcn_(&(h->entry), st_);			// cleanup
		// unlink from the hash table 
		unlink_node(h);
		// reset the hash node
		h->index = -1;
		h->prev = 0;
		h->next = 0;
	} 
	return h;
}

/*
 * Find the hnode based on the key. Return the hash node if the entry exists,
 * or NULL otherwise. Note that ret_index is set to the index that has been
 * hashed.
 */
template<class T> 
typename HashTable<T>::HASH_NODE * 
HashTable<T>::find_node_row(const T* entry, int* ret_index) {

	HASH_NODE* hnode;
	int index;

	// get the row
	index = hash_fcn_(entry) % num_row_;
	*ret_index = index;

	// compare the hnodes along the row
	for (hnode = table_[index]; hnode != 0; hnode = hnode->next) {
		if (cmp_fcn_(&(hnode->entry), entry)) {		// found the node
			// return the node
			return hnode;
		}
	}

	// not found
	return NULL;
}

/*
 * Expire function - it's called to free a node. Here, we only expire one node
 * at a time.
 */
template<class T>
void HashTable<T>::expire(double cur_ts) {
	
	HASH_NODE* h = (fptr_ == NULL) ? gtail_ : fptr_->gprev;

	if (h != NULL && cur_ts - h->ts > expire_timeout_) {
		// perform the cleanup function if necessary
		free_fcn_(&(h->entry), st_);
		// unlink from the hash table 
		unlink_node(h);
		// reset the hash node
		h->index = -1;
		h->prev = 0;
		h->next = 0;
		// update the free pointer
		fptr_ = h;
	}
}

/*
 * public instance member functions
 */

/*
 * Constructor - create a new hash table
 * 
 * @param name - name of the hash table
 * @param maxnum - max of entries in a hash table
 * @param expire_timeout - value of expire timeout
 * @param hash_fcn - user's hash function
 * @param cmp_fcn - user's compare function to check if two keys are equal
 * @param init_fcn - user's initialization function for a new entry
 * @param free_fcn - user's cleanup function for a recycled entry
 * @param st - the object (i.e., state table) that initializes this hash table
 */
template<class T> 
HashTable<T>::HashTable(const char* name, uint32_t maxnum, 
		double expire_timeout, HASH_FCN hash_fcn, CMP_FCN cmp_fcn, 
		INIT_FCN init_fcn, FREE_FCN free_fcn, void* st) {
	uint32_t i;

	// determine the right number of rows (make sure it is a prime number)

	// set the fields
	strncpy(name_, name, sizeof(name_));
	num_row_ = calc_next_prime(maxnum) * NUM_ROW_FACTOR;
	maxnum_ = maxnum;
	expire_timeout_ = expire_timeout;
	hash_fcn_ = hash_fcn;
	cmp_fcn_ = cmp_fcn;
	init_fcn_ = init_fcn;
	free_fcn_ = free_fcn;
	st_ = st;

	// create table
	table_ = (HASH_NODE**)calloc(num_row_, sizeof(HASH_NODE*));
	if (table_ == 0) {
		fprintf(stderr, "out of memory in HashTable - table (%s)", name_);
		exit(-1);
	}
	
	// create the global list
	ghead_ = (HASH_NODE*)calloc(maxnum_, sizeof(HASH_NODE));
	if (ghead_ == 0) {
		fprintf(stderr, "out of memory in HashTable - list (%s)", name_);
		exit(-1);
	}
	for (i=0; i<maxnum; ++i) {
		init_fcn_(&(ghead_[i].entry), st_);		// initialize the entry 
		ghead_[i].index = -1;
		ghead_[i].prev = 0;
		ghead_[i].next = 0;
		ghead_[i].gprev = (i == 0) ? 0 : &(ghead_[i-1]);
		ghead_[i].gnext = (i == maxnum-1) ? 0 : &(ghead_[i+1]);
	}
	gtail_ = &(ghead_[maxnum-1]);
	fptr_ = ghead_;
}

/*
 * Find the entry. 
 * 
 * @param entry - the entry to be found 
 * @param ts - the current timestamp
 * @param add_flag - if true, entry will be added if it's not found
 *                   if false, entry won't be added if it's not found
 * @return - if add_flag == true, return the pointer to the entry for the
 *           found/added entry
 *         - if add_flag == false, return the pointer to the entry for the 
 *           found entry, or 0 if the entry not found
 */
template<class T>
T* HashTable<T>::find(const T* entry, double ts, bool add_flag) {
	HASH_NODE* hnode;
	int index;
	T* ret;

	// find the node
	hnode = find_node_row(entry, &index);
	
	if (hnode != 0) {			// if the entry is found
		// move the node to the front of row list
		/*
		unlink_node(hnode);
		link_node(hnode);
		*/

		// set the return value
		ret = &(hnode->entry);

	} else {					// if the entry is not found
		if (add_flag) {
			// get a free node from the list
			hnode = get_free_node();
			// set the index
			hnode->index = index;
			// move the node to the front of row list
			link_node(hnode);
			// set return value	
			ret = &(hnode->entry);
		} else {
			ret = NULL;	
		}
	}

	// call the expire function - we assume find() is frequently called so
	// that the hash node is freed as soon as it expires. We do the link
	// operations only GLINK_PERIOD since the last one.
	if (expire_timeout_ > 0) {
		if (hnode != NULL && ts - hnode->ts > GLINK_PERIOD) {
			// move the node to the front of global list, only necessary when
			// expire is used
			gunlink_node(hnode);
			glink_node(hnode);

			// update the timestamp
			hnode->ts = ts;
		}
		expire(ts);
	}

	// return the entry	
	return ret;
}

/*
 * Return true if the table is full
 */
template<class T>
bool HashTable<T>::is_full() {
	return (fptr_ == NULL);
}

/*
 * reset all the hash entries in the hash table to the initial stage
 */
template<class T>
void HashTable<T>::reset() {
	HASH_NODE* h = (fptr_ == NULL) ? gtail_ : fptr_->gprev;

	// free the nodes
	while (h != NULL) {
		free_fcn_(&(h->entry), st_);
		h->index = -1;
		h->prev = 0;
		h->next = 0;
		h = h->gprev;
	}
	// reset the free pointer
	fptr_ = ghead_;

	// reset the table pointers
	memset(table_, 0, num_row_ * sizeof(HASH_NODE*));
}

#endif
