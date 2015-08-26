#ifndef __COMMON_HASH_HASH_FUNC_HH__
#define __COMMON_HASH_HASH_FUNC_HH__

class HashFunc {
public:
	static unsigned int hash( const char* data, unsigned int n ) {
		unsigned int hash = 388650013;
		unsigned int scale = 388650179;
		unsigned int hardener  = 1176845762;
		while ( n ) {
			hash *= scale;
			hash += *data++;
			n--;
		}
		return hash ^ hardener;
	}

	static unsigned int hash( const char* data1, unsigned int n1, const char* data2, unsigned int n2 ) {
		unsigned int hash = 388650013;
		unsigned int scale = 388650179;
		unsigned int hardener  = 1176845762;
		unsigned int n = n1 + n2;
		while ( n ) {
			hash *= scale;
			if ( n < n1 )
				hash += *data1++;
			else
				hash += *data2++;
			n--;
		}
		return hash ^ hardener;
	}

	static unsigned int hash( const char* data1, unsigned int n1, const char* data2, unsigned int n2, const char* data3, unsigned int n3 ) {
		unsigned int hash = 388650013;
		unsigned int scale = 388650179;
		unsigned int hardener  = 1176845762;
		unsigned int n = n1 + n2 + n3;
		while ( n ) {
			hash *= scale;
			if ( n < n1 )
				hash += *data1++;
			else if ( n < n1 + n2 )
				hash += *data2++;
			else
				hash += *data3++;
			n--;
		}
		return hash ^ hardener;
	}
};

#endif
