#include <cstdlib>
#include <vector>
#include "../../../common/config/global_config.hh"
#include "../../../common/ds/bitmask_array.hh"
#include "../../../common/coding/coding_scheme.hh"
#include "../../../common/coding/all_coding.hh"
#include "../../../common/coding/coding_handler.hh"

#define CHUNK_SIZE (4096)
#define C_K (4)
#define C_M (2)
#define FAIL (1)
#define FAIL2 (2)
#define FAIL3 (5)

struct CodingHandler handle;

void usage( char* argv ) {
    fprintf( stderr, "Usage: %s [raid5|cauchy|rdp|rs|evenodd]\n", argv);
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
        handle.scheme = CS_RAID5;
    else if ( strcmp ( arg, "cauchy" ) == 0 )
        handle.scheme = CS_CAUCHY;
    else if ( strcmp ( arg, "rdp" ) == 0 )
        handle.scheme = CS_RDP;
    else if ( strcmp ( arg, "rs" ) == 0 )
        handle.scheme = CS_RS;
    else if ( strcmp ( arg, "evenodd" ) == 0 )
        handle.scheme = CS_EVENODD;
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
        bitmap.set ( idx  , 0 );
    }

    std::vector< uint32_t > failed;

    switch ( handle.scheme ) {
        case CS_RAID5:
            handle.raid5 = new Raid5Coding2( C_K, CHUNK_SIZE );
            printf( ">> encode K: %d   M: 1 \n", C_K );
            handle.raid5->encode ( chunks, chunks[ C_K ], 1 );
            failed.push_back( FAIL );
            break;
        case CS_RDP:
            handle.rdp = new RDPCoding( C_K, CHUNK_SIZE );
            printf( ">> encode K: %d   M: 2 \n", C_K );
            handle.rdp->encode ( chunks, chunks[ C_K ], 1 );
            handle.rdp->encode ( chunks, chunks[ C_K + 1 ], 2 );
            failed.push_back( FAIL );
            failed.push_back( FAIL2 );
            //printChunk( chunks[ C_K ] , C_K);
            //printChunk( chunks[ C_K + 1 ] , C_K + 1);
            break;
        case CS_CAUCHY:
            handle.cauchy = new CauchyCoding( C_K, C_M, CHUNK_SIZE );
            printf( ">> encode K: %d   M: %d \n", C_K, C_M );
            for (uint32_t idx = 0 ; idx < C_M ; idx ++ ) {
                handle.cauchy->encode ( chunks, chunks[ C_K + idx ], idx + 1 );
                //printChunk( chunks[ C_K + idx ] , C_K + idx );
            }
            failed.push_back( FAIL );
            failed.push_back( FAIL2 );
            failed.push_back( FAIL3 );
            break;
        case CS_RS:
            handle.rs = new RSCoding( C_K, C_M, CHUNK_SIZE );
            printf( ">> encode K: %d   M: %d \n", C_K, C_M );
            for (uint32_t idx = 0 ; idx < C_M ; idx ++ ) {
                handle.rs->encode ( chunks, chunks[ C_K + idx ], idx + 1 );
                //printChunk( chunks[ C_K + idx ] , C_K + idx );
            }
            failed.push_back( FAIL );
            failed.push_back( FAIL2 );
            failed.push_back( FAIL3 );
            break;
        case CS_EVENODD:
            handle.evenodd = new EvenOddCoding( C_K, CHUNK_SIZE );
            printf( ">> encode K: %d   M: 2 \n", C_K );
            handle.evenodd->encode ( chunks, chunks[ C_K ], 1 );
            handle.evenodd->encode ( chunks, chunks[ C_K + 1 ], 2 );
            //printChunk( chunks[ C_K ] , C_K );
            //printChunk( chunks[ C_K + 1 ] , C_K + 1 );
            failed.push_back( FAIL );
            failed.push_back( FAIL2 );
            break;
        default:
            return -1;
    }


    memset ( readbuf , 0, CHUNK_SIZE * C_M ); 
    
    printf( ">> fail disk %d ... ", failed[ 0 ] );
    chunks[ failed[ 0 ] ]->data = readbuf;
    bitmap.unset ( failed[ 0 ] , 0 );

    switch ( handle.scheme ) {
        case CS_RAID5:
            handle.raid5->decode ( chunks, &bitmap );
            break;
        case CS_RDP:
            handle.rdp->decode ( chunks, &bitmap );
            break;
        case CS_CAUCHY:
            handle.cauchy->decode ( chunks, &bitmap );
            break;
        case CS_RS:
            handle.rs->decode ( chunks, &bitmap );
            break;
        case CS_EVENODD:
            handle.evenodd->decode ( chunks, &bitmap );
            break;
        default:
            return -1;
    }

    if ( memcmp ( readbuf, buf + FAIL * CHUNK_SIZE, CHUNK_SIZE ) == 0 ) {
        fprintf( stdout, "Data recovered\n" );
    } else {
        fprintf( stdout, "FAILED to recover data!! O: [%x] R: [%x]\n", *(buf + FAIL * CHUNK_SIZE), *(readbuf) );
    }

    // double failure
    if ( handle.scheme != CS_RAID5 ) {
        memset ( readbuf , 0, CHUNK_SIZE * C_M ); 
        printf( ">> fail disk %d, ", failed[ 0 ]);
        bitmap.unset ( failed[ 0 ], 0 );
        printf( "%d ... ", failed[ 1 ]);
        bitmap.unset ( failed[ 1 ], 0 );
        chunks[ failed[ 1 ] ]->data = readbuf + CHUNK_SIZE;
        switch ( handle.scheme ) {
            case CS_RDP:
                handle.rdp->decode ( chunks, &bitmap );
                break;
            case CS_CAUCHY:
                handle.cauchy->decode ( chunks, &bitmap );
                break;
            case CS_RS:
                handle.rs->decode ( chunks, &bitmap );
                break;
            case CS_EVENODD:
                handle.evenodd->decode ( chunks, &bitmap );
                break;
            default:
                return -1;
        }
        if ( memcmp ( readbuf, buf + failed[ 0 ] * CHUNK_SIZE, CHUNK_SIZE ) == 0 ) {
            fprintf( stdout, "Data %d recovered ... ", failed[ 0 ] );
        } else {
            fprintf( stdout, "\nFAILED to recover data!! [%x] \n", *(buf + failed[ 0 ] * CHUNK_SIZE));
            printChunk( chunks[ failed[ 0 ] ], failed[ 0 ] );
        }
        if ( memcmp ( readbuf + CHUNK_SIZE , buf + ( failed[ 1 ] ) * CHUNK_SIZE , CHUNK_SIZE ) == 0 ) {
            fprintf( stdout, "Data %d recovered\n", failed[ 1 ] );
        } else {
            fprintf( stdout, "FAILED to recover data!! [%x] \n", *(buf + failed[ 1 ] * CHUNK_SIZE));
            printChunk( chunks[ failed[ 1 ] ], failed[ 1 ] );
        }
    }

    // triple failure
    if ( handle.scheme != CS_RAID5 && handle.scheme != CS_RDP && handle.scheme != CS_EVENODD && C_M > 2) {
        memset ( readbuf , 0, CHUNK_SIZE * C_M ); 
        printf( ">> fail disk %d, ", failed[ 0 ]);
        bitmap.unset ( failed[ 0 ], 0 );
        printf( "%d, ", failed[ 1 ]);
        bitmap.unset ( failed[ 1 ], 0 );
        printf( "%d ... ", failed[ 2 ]);
        bitmap.unset ( failed[ 2 ], 0 );
        chunks[ failed[ 2 ] ]->data = readbuf + CHUNK_SIZE * 2;
        switch ( handle.scheme ) {
            case CS_CAUCHY:
                handle.cauchy->decode ( chunks, &bitmap );
                break;
            case CS_RS:
                handle.rs->decode ( chunks, &bitmap );
                break;
            default:
                return -1;
        }
        if ( memcmp ( readbuf, buf + failed[ 0 ] * CHUNK_SIZE, CHUNK_SIZE ) == 0 ) {
            fprintf( stdout, "Data %d recovered ... ", failed[ 0 ] );
        } else {
            fprintf( stdout, "\nFAILED to recover data!! [%x] \n", *(buf + failed[ 0 ] * CHUNK_SIZE));
            printChunk( chunks[ failed[ 0 ] ], failed[ 0 ] );
        }
        if ( memcmp ( readbuf + CHUNK_SIZE , buf + ( failed[ 1 ] ) * CHUNK_SIZE , CHUNK_SIZE ) == 0 ) {
            fprintf( stdout, "Data %d recovered ... ", failed[ 1 ] );
        } else {
            fprintf( stdout, "FAILED to recover data!! [%x] \n", *(buf + failed[ 1 ] * CHUNK_SIZE));
            printChunk( chunks[ failed[ 1 ] ], failed[ 1 ] );
        }
        if ( memcmp ( readbuf + CHUNK_SIZE * 2, buf + ( failed[ 2 ] ) * CHUNK_SIZE , CHUNK_SIZE ) == 0 ) {
            fprintf( stdout, "Data %d recovered\n", failed[ 2 ] );
        } else {
            fprintf( stdout, "FAILED to recover data!! [%x] \n", *(buf + failed[ 2 ] * CHUNK_SIZE));
            printChunk( chunks[ failed[ 2 ] ], failed[ 2 ] );
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
