#include <cstdlib>
#include "../../../common/config/global_config.hh"
#include "../../../common/ds/bitmask_array.hh"
#include "../../../common/coding/coding_scheme.hh"
#include "../../../common/coding/all_coding.hh"
#include "../../../common/coding/coding_handler.hh"

#define CHUNK_SIZE (4096)
#define C_K (9)
#define C_M (2)
#define FAIL (2)
#define FAIL2 (0)

CodingScheme scheme;

void usage( char* argv ) {
    fprintf( stderr, "Usage: %s [raid5|cauchy|rdp]\n", argv);
}

void printChunk( Chunk *chunk, uint32_t id = 0 ) {
    char *prev = chunk->data;
    int prevPos = 0;
    if (id != 0) {
        printf(" chunk %d:\n", id);
    }
    for (uint32_t i = 1; i < CHUNK_SIZE; i++) {
        if ( chunk->data[i] != *prev ) {
            printf("[%x] from %d to %d\n", *prev, prevPos, i-1);
            prevPos = i;
            prev = &chunk->data[i];
        }
    }
    printf("[%x] from %d to %d\n", *prev, prevPos, CHUNK_SIZE - 1);
}

bool parseInput( char* arg ) {
    if ( strcmp ( arg, "raid5" ) == 0 )
        scheme = CS_RAID5;
    else if ( strcmp ( arg, "cauchy" ) == 0 )
        scheme = CS_CAUCHY;
    else if ( strcmp ( arg, "rdp" ) == 0 )
        scheme = CS_RDP;
    else
        return false;

    return true;
}

int main( int argc, char **argv ) {

    if ( argc < 2 ) {
        usage( argv[ 0 ] );
        return 0;
    }

    if ( parseInput( argv[ 1 ] ) == false ) {
        usage( argv[ 0 ]);
        return -1;
    }

    char* buf = ( char* ) malloc ( sizeof( char ) * CHUNK_SIZE * ( C_K + C_M ) );
    char* readbuf = ( char* ) malloc ( sizeof( char ) * CHUNK_SIZE * ( C_M ) );
    Chunk ** chunks = ( Chunk ** ) malloc ( sizeof( Chunk* ) * ( C_K + C_M ) );
    BitmaskArray bitmap ( 1, C_K + C_M );

    // set up the chunks
    for ( uint32_t idx = 0 ; idx < C_K * 2 ; idx ++ ) {
        memset( buf + idx * CHUNK_SIZE / 2, idx / 2 * 3 + 5 , CHUNK_SIZE / 2 );
        if ( idx % 2 == 0 ) {
            chunks[ idx / 2 ] = ( Chunk* ) malloc ( sizeof( Chunk ) );
            chunks[ idx / 2 ]->data = buf + ( idx / 2 ) * CHUNK_SIZE;

            // mark the chunk status
            bitmap.set ( idx / 2 , 0 );
        }
    }

    for ( uint32_t idx = C_K ; idx < C_K + C_M ; idx ++ ) {
        memset( buf + idx * CHUNK_SIZE, 0, CHUNK_SIZE );
        chunks[ idx ] = ( Chunk* ) malloc ( sizeof( Chunk ) );
        chunks[ idx ]->data = buf + idx * CHUNK_SIZE;
        bitmap.set ( idx  , 1 );
    }

    CodingHandler handle;
    handle.scheme = scheme;

    switch ( scheme ) {
        case CS_RAID5:
            handle.raid5 = new Raid5Coding2( C_K, CHUNK_SIZE );
            printf( ">> encode K: %d   M: 1 \n", C_K );
            handle.raid5->encode ( chunks, chunks[ C_K ], 1 );
            break;
        case CS_RDP:
            handle.rdp = new RDPCoding( C_K, CHUNK_SIZE );
            printf( ">> encode K: %d   M: 2 \n", C_K );
            handle.rdp->encode ( chunks, chunks[ C_K ], 1 );
            handle.rdp->encode ( chunks, chunks[ C_K + 1 ], 2 );
            //printChunk( chunks[ C_K ] , C_K);
            //printChunk( chunks[ C_K + 1 ] , C_K + 1);
            break;
        default:
            return -1;
    }


    memset ( readbuf , 0, CHUNK_SIZE * C_M ); 
    
    printf( ">> fail disk %d\n", FAIL );
    chunks[ FAIL ]->data = readbuf;
    bitmap.unset ( FAIL , 0 );

    printf( ">> decode for disk %d\n", FAIL );
        
    switch ( scheme ) {
        case CS_RAID5:
            handle.raid5->decode ( chunks, &bitmap );
            break;
        case CS_RDP:
            handle.rdp->decode ( chunks, &bitmap );
            break;
        default:
            return -1;
    }

    if ( memcmp ( readbuf, buf + FAIL * CHUNK_SIZE, CHUNK_SIZE ) == 0 ) {
        fprintf( stdout, "Data recovered\n" );
    } else {
        fprintf( stdout, "FAILED to recover data!! O: [%x] R: [%x]\n", *(buf + FAIL * CHUNK_SIZE), *(readbuf) );
    }

    if ( scheme == CS_RDP ) {
        memset ( readbuf , 0, CHUNK_SIZE * C_M ); 
        printf( ">> fail disk %d\n", FAIL );
        bitmap.unset ( FAIL , 0 );
        printf( ">> fail disk %d\n", FAIL2);
        bitmap.unset ( FAIL2, 0 );
        chunks[ FAIL2 ]->data = readbuf + CHUNK_SIZE;
        handle.rdp->decode ( chunks, &bitmap );
        if ( memcmp ( readbuf, buf + FAIL * CHUNK_SIZE, CHUNK_SIZE ) == 0 ) {
            fprintf( stdout, "Data recovered\n" );
        } else {
            fprintf( stdout, "FAILED to recover data!! O: [%x] R: [%x]\n", *(buf + FAIL * CHUNK_SIZE), *(readbuf) );
            printChunk( chunks[ FAIL ], FAIL );
        }
        if ( memcmp ( readbuf + CHUNK_SIZE , buf + ( FAIL2 ) * CHUNK_SIZE , CHUNK_SIZE ) == 0 ) {
            fprintf( stdout, "Data recovered\n" );
        } else {
            fprintf( stdout, "FAILED to recover data!! O: [%x] R: [%x]\n", *(buf + ( FAIL2 ) * CHUNK_SIZE), *(readbuf + CHUNK_SIZE) );
            printChunk( chunks[ FAIL2 ], FAIL2 );
        }
    }

    // clean up
    //free( buf );
    //free( readbuf );
    //for ( uint32_t idx = 0 ; idx < C_M + C_K ; idx ++ ) {
    //    chunks[ idx ]->data = NULL;
    //    free(chunks[ idx ]);
    //}
    //free( chunks );

    return 0;
}
