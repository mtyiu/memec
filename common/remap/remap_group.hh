#ifndef __COMMON_REMAP_REMAP_GROUP_HH__
#define __COMMON_REMAP_REMAP_GROUP_HH__

#define COORD_GROUP     "coordinators"
#define CLIENT_GROUP    "clients"
#define SERVER_GROUP     "servers"

#define COORD_GROUP_LEN     ( sizeof( COORD_GROUP ) - 1 )
#define CLIENT_GROUP_LEN    ( sizeof( CLIENT_GROUP ) - 1 )
#define SERVER_GROUP_LEN     ( sizeof( SERVER_GROUP ) - 1 )

#define CLIENT_PREFIX   "client"
#define COORD_PREFIX    "coord"
#define SLAVE_PREFIX    "server"

#define COORD_PREFIX_LEN    ( sizeof( COORD_PREFIX ) - 1 )
#define CLIENT_PREFIX_LEN   ( sizeof( CLIENT_PREFIX ) - 1 )
#define SLAVE_PREFIX_LEN    ( sizeof( SLAVE_PREFIX ) - 1 )

#endif
