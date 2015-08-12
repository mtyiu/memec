#include <cstdlib>
#include <vector>
#include "../../../common/config/global_config.hh"
#include "../../../common/ds/bitmask_array.hh"
#include "../../../common/coding/coding_scheme.hh"
#include "../../../common/coding/all_coding.hh"
#include "../../../common/coding/coding_handler.hh"
#include "../../../common/util/time.hh"

#define CHUNK_SIZE (8192)
#define C_K (12)
#define C_M (2)
#define FAIL (2)
#define FAIL2 (3)
#define FAIL3 (4)
#define ROUNDS (5000)

struct CodingHandler handle;

void usage( char* argv ) {
    fprintf( stderr, "Usage: %s [raid5|cauchy|rdp|rs|evenodd]\n", argv);
}

void report( double duration ) {
    printf( " %.4lf Kop/s\t", ROUNDS / 1000 / duration );
    printf( " %.4lf MB/s\n", C_K * CHUNK_SIZE / 1024.0 / 1024.0 * ROUNDS / duration );
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
    failed.push_back( FAIL );
    failed.push_back( FAIL2 );
    failed.push_back( FAIL3 );
    switch ( handle.scheme ) {
        case CS_RAID5:
            handle.raid5 = new Raid5Coding2( C_K, CHUNK_SIZE );
            printf( ">> encode K: %d   M: 1   ", C_K );
            break;
        case CS_RDP:
            handle.rdp = new RDPCoding( C_K, CHUNK_SIZE );
            printf( ">> encode K: %d   M: 2   ", C_K );
            break;
        case CS_CAUCHY:
            handle.cauchy = new CauchyCoding( C_K, C_M, CHUNK_SIZE );
            printf( ">> encode K: %d   M: %d   ", C_K, C_M );
            break;
        case CS_RS:
            handle.rs = new RSCoding( C_K, C_M, CHUNK_SIZE );
            printf( ">> encode K: %d   M: %d   ", C_K, C_M );
            break;
        case CS_EVENODD:
            handle.evenodd = new EvenOddCoding( C_K, CHUNK_SIZE );
            printf( ">> encode K: %d   M: 2   ", C_K );
            break;
        default:
            return -1;
    }

    struct timespec st = start_timer();
    uint32_t m = 0;
    for ( uint32_t round = 0 ; round < ROUNDS ; round ++ ) {
        switch ( handle.scheme ) {
            case CS_RAID5:
                handle.raid5->encode ( chunks, chunks[ C_K ], 1 );
                m = 1;
                break;
            case CS_RDP:
                handle.rdp->encode ( chunks, chunks[ C_K ], 1 );
                handle.rdp->encode ( chunks, chunks[ C_K + 1 ], 2 );
                m = 2;
                break;
            case CS_CAUCHY:
                for (uint32_t idx = 0 ; idx < C_M ; idx ++ ) {
                    handle.cauchy->encode ( chunks, chunks[ C_K + idx ], idx + 1 );
                }
                m = C_M;
                break;
            case CS_RS:
                for (uint32_t idx = 0 ; idx < C_M ; idx ++ ) {
                    handle.rs->encode ( chunks, chunks[ C_K + idx ], idx + 1 );
                }
                m = C_M;
                break;
            case CS_EVENODD:
                handle.evenodd->encode ( chunks, chunks[ C_K ], 1 );
                handle.evenodd->encode ( chunks, chunks[ C_K + 1 ], 2 );
                m = 2;
                break;
            default:
                return -1;
        }
    }
    // report time per encoding operations
    report( get_elapsed_time( st ) / m );

    memset ( readbuf , 0, CHUNK_SIZE * C_M ); 
    
    printf( ">> fail disk %d ... ", failed[ 0 ] );
    chunks[ failed[ 0 ] ]->data = readbuf;
    bitmap.unset ( failed[ 0 ] , 0 );

    st = start_timer();
    for ( uint32_t round = 0 ; round < ROUNDS ; round ++ ) {
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
    }
    report( get_elapsed_time( st ) );

    // double failure
    if ( handle.scheme != CS_RAID5 ) {
        memset ( readbuf , 0, CHUNK_SIZE * C_M ); 
        printf( ">> fail disk %d, ", failed[ 0 ]);
        bitmap.unset ( failed[ 0 ], 0 );
        printf( "%d ... ", failed[ 1 ]);
        bitmap.unset ( failed[ 1 ], 0 );
        chunks[ failed[ 1 ] ]->data = readbuf + CHUNK_SIZE;

        st = start_timer();
        for ( uint32_t round = 0 ; round < ROUNDS ; round ++ ) {
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
        }
        report( get_elapsed_time( st ) );
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
        st = start_timer();
        for ( uint32_t round = 0 ; round < ROUNDS ; round ++ ) {
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
        }
        report( get_elapsed_time( st ) );

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
