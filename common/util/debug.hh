#ifndef __COMMON_UTIL_DEBUG_HH__
#define __COMMON_UTIL_DEBUG_HH__

#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define BLACK  		"\x1B[0m"
#define RED    		"\x1B[31m"
#define GREEN  		"\x1B[32m"
#define YELLOW 		"\x1B[33m"
#define BLUE   		"\x1B[34m"
#define MAGENTA		"\x1B[35m"
#define CYAN   		"\x1B[36m"
#define WHITE  		"\x1B[37m"
#define RESET  		"\033[0m"

#define DEBUG_STR_BUF_SIZE	4096

// #define PRINT_DEBUG_MESSAGE

#ifdef PRINT_DEBUG_MESSAGE

#define __DEBUG__(color, class_name, func, ...) do { \
		int _len; \
		char _buf[ DEBUG_STR_BUF_SIZE ]; \
		_len = snprintf( _buf, DEBUG_STR_BUF_SIZE, "%s[%s::%s()] ", color, \
		                class_name, func ); \
		_len += snprintf( _buf + _len, DEBUG_STR_BUF_SIZE - _len, __VA_ARGS__ ); \
		if ( DEBUG_STR_BUF_SIZE - _len > ( int ) strlen( RESET ) ) \
			_len += snprintf( _buf + _len, DEBUG_STR_BUF_SIZE - _len, RESET ); \
		_len += snprintf( _buf + _len, DEBUG_STR_BUF_SIZE - _len, "\n" ); \
		if ( ::write( 2, _buf, _len ) != _len ) { \
			fprintf( stderr, "Cannot write debug message.\n" ); \
		} \
	} while( 0 )

#define __ERROR__(class_name, func, ...) \
	__DEBUG__(RED, class_name, func, __VA_ARGS__)

#else

#define __DEBUG__(color, class_name, func, ...)
#define __ERROR__(class_name, func, ...)

#endif

#endif
