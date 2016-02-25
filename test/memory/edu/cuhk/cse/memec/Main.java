package edu.cuhk.cse.memec;

import java.util.HashMap;
import java.util.Random;

public class Main implements Runnable {
	/* Configuration */
	public static int keySize, chunkSize, port;
	public static String host;
	/* Test parameters */
	public static int numThreads;
	public static int workload;
	public static long totalSize;
	/* States */
	public static Main[] mainObjs;
	public static Thread[] threads;
	public static boolean[] completed;
	public static byte phase;
	public static Object lock;
	/* Constants */
	private static final String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	private static final int charactersLength = characters.length();

	/* Instance variables */
	private int id;
	private MemEC memec;
	private String[] keys;
	private HashMap<String, Integer> map;
	private Random random;
	public long[] size;
	public long[] count;

	public Main( int id, int fromId, int toId ) {
		this.id = id;
		this.memec = new MemEC( Main.keySize, Main.chunkSize, Main.host, Main.port, fromId, toId );
		this.map = new HashMap<String, Integer>();
		this.random = new Random();
		this.size = new long[ 3 ];
		this.count = new long[ 3 ];
		for ( int i = 0; i < 3; i++ ) {
			this.size[ i ] = 0;
			this.count[ i ] = 0;
		}
	}

	private String generate( int size, boolean toLower ) {
		StringBuilder sb = new StringBuilder( size );
		for ( int i = 0; i < size; i++ ) {
			sb.append( Main.characters.charAt( this.random.nextInt( Main.charactersLength ) ) );
		}
		String ret = sb.toString();
		return toLower ? ret.toLowerCase() : ret;
	}

	public void run() {
		if ( ! memec.connect() )
			return;

		int rand, size;
		int i = 0, keySize, valueSize;
		long target = Main.totalSize / Main.numThreads;
		String key, value;
		boolean ret;
		byte phase = 1;

		while ( phase > 0 ) {
			if ( phase == 1 ) {
				while( this.size[ 0 ] < target ) {
					// Determine key and value size
					keySize = 32;
					switch( Main.workload ) {
						case 1:
						case 2:
						case 3:
							valueSize = 68;
							break;
						case 4:
						case 5:
							valueSize = this.random.nextInt( 50 ) + 100 - 32;
							break;
						case 6:
							valueSize = this.random.nextInt( 100 ) + 100 - 32;
							break;
						case 7:
							valueSize = this.random.nextInt( 1000 ) + 1000 - 32;
							break;
						case 8:
							valueSize = this.random.nextInt( 100 ) + 50 - 32;
							break;
						default:
							return;
					}

					// Generate a key-value pair
					do {
						key = this.generate( keySize, false );
					} while ( this.map.containsKey( key ) );
					value = this.generate( valueSize, true );

					// Perform SET operation
					ret = memec.set( key, value );

					if ( ret ) {
						// Update internal key-value map
						this.map.put( key, valueSize );

						// Update counter
						this.size[ 0 ] += keySize + valueSize;
						this.count[ 0 ]++;
					}
				}

				i = 0;
				this.keys = new String[ this.map.size() ];
				for ( String k : this.map.keySet() ) {
					this.keys[ i++ ] = k;
				}

				Main.complete( this.id );
				phase = 2;
			} else if ( phase == 2 ) {
				int index;
				int removeTarget;

				switch( Main.workload ) {
					case 1:
						removeTarget = 0;
						break;
					case 2:
					case 4:
					case 6:
						removeTarget = ( int )( this.count[ 0 ] * 0.1 );
						break;
					case 3:
					case 5:
					case 8:
						removeTarget = ( int )( this.count[ 0 ] * 0.9 );
						break;
					case 7:
						removeTarget = ( int )( this.count[ 0 ] * 0.5 );
						break;
					default:
						return;
				}

				while ( this.count[ 1 ] < removeTarget ) {
					// Select a key to delete
					do {
						index = this.random.nextInt( this.keys.length );
						key = this.keys[ index ];
					} while ( key == null );
					valueSize = this.map.get( key );

					// Perform DELETE operation
					ret = memec.delete( key );

					if ( ret ) {
						// Update internal key-value map
						this.keys[ index ] = null;
						this.map.remove( key );

						// Update counter
						this.size[ 1 ] += key.length() + valueSize;
						this.count[ 1 ]++;
					}
				}

				this.keys = null;
				Main.complete( this.id );
				phase = 3;
			} else if ( phase == 3 ) {
				target = target - this.size[ 0 ] + this.size[ 1 ];
				while( this.size[ 2 ] < target ) {
					// Determine key and value size
					keySize = 32;
					switch( Main.workload ) {
						case 1:
						case 2:
						case 3:
							valueSize = 98;
							break;
						case 4:
						case 5:
						case 6:
							valueSize = this.random.nextInt( 50 ) + 200 - 32;
							break;
						case 7:
							valueSize = this.random.nextInt( 1000 ) + 500 - 32;
							break;
						case 8:
							valueSize = this.random.nextInt( 3000 ) + 1000 - 32;
							break;
						default:
							return;
					}

					// Generate a key-value pair
					do {
						key = this.generate( keySize, false );
					} while ( this.map.containsKey( key ) );
					value = this.generate( valueSize, true );

					// Perform SET operation
					ret = memec.set( key, value );

					if ( ret ) {
						// Update internal key-value map
						this.map.put( key, valueSize );

						// Update counter
						this.size[ 2 ] += keySize + valueSize;
						this.count[ 2 ]++;
					}
				}
				Main.complete( this.id );
				phase = 0; // Terminate
			} else {
				System.out.println( phase );
			}
		}
	}

	public static void complete( int id ) {
		boolean ret = true;
		int count = 0;
		synchronized( Main.lock ) {
			Main.completed[ id ] = true;
			for ( int i = 0; i < Main.numThreads; i++ ) {
				if ( ! Main.completed[ i ] ) {
					ret = false;
					break;
				} else {
					count++;
				}
			}
			System.out.print( "\r[Phase " + Main.phase + "] Number of threads completed: " + count );
			if ( ret ) {
				System.out.println();
				if ( Main.phase == 3 )
					return;
				Main.phase++;
				for ( int i = 0; i < Main.numThreads; i++ ) {
					Main.completed[ i ] = false;
					// Wake up the thread
					Main.lock.notifyAll();
				}
			} else {
				// Sleep until all other threads complete
				if ( Main.phase < 3 ) {
					try {
						Main.lock.wait();
					} catch( InterruptedException e ) {}
				}
			}
		}
	}

	public static void main( String[] args ) throws Exception {
		if ( args.length < 7 ) {
			System.err.println( "Required parameters: [Maximum Key Size] [Chunk Size] [Hostname] [Port Number] [Workload No.] [Number of Threads] [Total Data Size]" );
			System.err.println(
				"\n" +
				"Summary of workloads:\n" +
				"---------------------\n" +
				"Workload\t                 Before\tDelete\t                  After\n" +
				"--------\t-----------------------\t------\t-----------------------\n" +
				"      W1\t        Fixed 100 bytes\t   N/A\t                    N/A\n" +
				"      W2\t        Fixed 100 bytes\t   10%\t        Fixed 130 bytes\n" +
				"      W3\t        Fixed 100 bytes\t   90%\t        Fixed 130 bytes\n" +
				"      W4\t  Uniform 100-150 bytes\t   10%\t  Uniform 200-250 bytes\n" +
				"      W5\t  Uniform 100-150 bytes\t   90%\t  Uniform 200-250 bytes\n" +
				"      W6\t  Uniform 100-200 bytes\t   10%\t  Uniform 200-250 bytes\n" +
				"      W7\tUniform 1000-2000 bytes\t   50%\tUniform 1500-2500 bytes\n" +
				"      W8\t   Uniform 50-150 bytes\t   90%\tUniform 1000-4000 bytes\n"
			);
			System.exit( 1 );
		}

		try {
			Main.keySize = Integer.parseInt( args[ 0 ] );
			Main.chunkSize = Integer.parseInt( args[ 1 ] );
			Main.host = args[ 2 ];
			Main.port = Integer.parseInt( args[ 3 ] );
			Main.workload = Integer.parseInt( args[ 4 ] );
			Main.numThreads = Integer.parseInt( args[ 5 ] );
			Main.totalSize = Long.parseLong( args[ 6 ] );
		} catch( NumberFormatException e ) {
			System.err.println( "Parameters: [Maximum Key Size], [Chunk Size], [Port Number], [Workload No.], [Number of threads], and [Total Data Size] should be integers." );
			System.exit( 1 );
			return;
		}

		/* Initialization */
		Main.mainObjs = new Main[ Main.numThreads ];
		Main.threads = new Thread[ Main.numThreads ];
		Main.completed = new boolean[ Main.numThreads ];
		Main.phase = 1;
		Main.lock = new Object();
		for ( int i = 0; i < Main.numThreads; i++ ) {
			int fromId = Integer.MAX_VALUE / Main.numThreads * i;
			int toId = Integer.MAX_VALUE / Main.numThreads * ( i + 1 );
			Main.mainObjs[ i ] = new Main( i, fromId, toId );
			Main.threads[ i ] = new Thread( Main.mainObjs[ i ] );
			Main.completed[ i ] = false;
		}

		/* Start execution */
		for ( int i = 0; i < numThreads; i++ ) {
			Main.threads[ i ].start();
		}

		for ( int i = 0; i < numThreads; i++ ) {
			Main.threads[ i ].join();
		}

		/* Report statistics */
		long[] size = new long[ 3 ];
		long[] count = new long[ 3 ];
		for ( int i = 0; i < numThreads; i++ ) {
			for ( int j = 0; j < 3; j++ ) {
				size[ j ] += Main.mainObjs[ i ].size[ j ];
				count[ j ] += Main.mainObjs[ i ].count[ j ];
			}
		}

		System.out.printf(
			"\n" +
			"Total data size:\n" +
			"(Phase 1) %d; (Phase 2) %d; (Phase 3) %d\n\n" +
			"Total number of operations:\n" +
			"(Phase 1) %d; (Phase 2) %d; (Phase 3) %d\n",
			size[ 0 ], size[ 1 ], size[ 2 ],
			count[ 0 ], count[ 1 ], count[ 2 ]
		);
	}
}
