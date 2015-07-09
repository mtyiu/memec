#include <signal.h>

class Signal {
public:
	static inline void setHandler( sighandler_t handler = 0 ) {
		int signals[] = {
			SIGINT
		}, len = sizeof( signals ) / sizeof( int );
		for ( int i = 0; i < len; i++ )
			signal( signals[ i ], handler ? handler : SIG_IGN );

		signal( SIGUSR1, SIG_IGN );
	}
};