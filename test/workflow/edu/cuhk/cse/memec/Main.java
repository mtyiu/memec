package edu.cuhk.cse.memec;

import java.util.Scanner;
import java.util.HashMap;
import java.util.Random;
import java.util.Map;
import java.util.Set;

public class Main implements Runnable {
	/* Configuration */
	public static int keySize, chunkSize, port;
	public static String host;
	/* Test parameters */
	public static int numThreads, numRecords, numOps;
	public static boolean fixedSize = true;
	/* States */
	public static Main[] mainObjs;
	public static Thread[] threads;
	public static int completedOps;
	public static Object lock;
	/* Constants */
	private static final String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	private static final int charactersLength = characters.length();

	/* Instance variables */
	private MemEC memec;
	private HashMap<String, String> map;
	private Random random;
	private int id;
	public int[] completed, succeeded;

	public Main( int fromId, int toId, int id ) {
		this.memec = new MemEC( Main.keySize, Main.chunkSize, Main.host, Main.port, fromId, toId );
		this.map = new HashMap<String, String>( Main.numRecords / Main.numThreads );
		this.completed = new int[ 4 ];
		this.succeeded = new int[ 4 ];
		this.random = new Random();
		this.id = id;
	}

	private String generate( int size, boolean toLower ) {
		StringBuilder sb = new StringBuilder( size );
		for ( int i = 0; i < size; i++ ) {
			sb.append( Main.characters.charAt( this.random.nextInt( Main.charactersLength ) ) );
		}
		String ret = sb.toString();
		return toLower ? ret.toLowerCase() : ret;
	}

	private int incrementCounter() {
		int ret;
		synchronized( Main.lock ) {
			this.completedOps++;
			ret = this.completedOps;
			if ( this.completedOps % ( Main.numOps / 100 ) == 0 )
				System.out.printf( "Completed operations: %d (%.2f%%)\r", this.completedOps, ( double ) this.completedOps / Main.numOps * 100.0 );
		}
		return ret;
	}

	public void run() {
		if ( ! memec.connect() )
			return;

		int rand, size;

		int i = 0, keySize, valueSize, index,
		    numOps = Main.numOps / Main.numThreads,
		    numRecords = Main.numRecords / Main.numThreads;
		String key, value;
		boolean ret;

		while( i < numOps ) {
			// rand = this.random.nextInt( 4 );
			rand = this.random.nextInt( 2 );
			size = this.map.size();
			ret = false;

			if ( rand == 1 ) rand = 2;

			/*
			if ( size != numRecords ) rand = 0;
			if ( i == numRecords * 3 / 4 ) {
				System.err.println( "[" + this.id + "] Sleep for 4 seconds." );
				try {
					Thread.sleep( 4000 );
				} catch ( Exception e ) {}

				// if ( rand == 0 ) rand = 1;
			}
			*/

			if ( rand == 0 ) {
				// SET
				if ( size < numRecords ) {
					// Construct new key-value pair
					keySize = Main.keySize >> 3;
					valueSize = Main.fixedSize ? ( Main.chunkSize >> 5 ) : this.random.nextInt( Main.chunkSize >> 3 );
					if ( valueSize < 4 ) valueSize = 4;

					// Store it in the HashMap
					do {
						key = this.generate( keySize, false );
					} while ( this.map.containsKey( key ) );
					value = this.generate( valueSize, true );

					// Issue the request
					ret = memec.set( key, value );

					if ( ret ) this.map.put( key, value );

					this.completed[ 0 ]++;
					if ( ret ) this.succeeded[ 0 ]++;
					this.incrementCounter();
					i++;
				}
			} else if ( size > 0 ) {
				// Retrieve one key-value pair in the HashMap
				Object[] entries;
				Map.Entry<String, String> entry;
				index = this.random.nextInt( size );
				entries = this.map.entrySet().toArray();
				entry = ( Map.Entry<String, String> ) entries[ index ];
				key = entry.getKey();
				value = entry.getValue();
				entries = null;

				if ( rand == 1 ) {
					// GET
					String v = memec.get( key );
					ret = ( v != null );

					if ( ret ) {
						ret = v.equals( value );
						if ( ! ret ) {
							System.out.println( "Value mismatch (key: " + key + "): (wrong) " + v.length() + " vs. (correct) " + value.length() );
						}
					}

					this.completed[ 1 ]++;
					if ( ret ) this.succeeded[ 1 ]++;
					this.incrementCounter();
					i++;
				} else if ( rand == 2 ) {
					// UPDATE
					int length, offset;

					do {
						length = value.length();
						offset = this.random.nextInt( length );
						length = this.random.nextInt( length - offset );
					} while ( length <= 0 );
					String valueUpdate = this.generate( length, true );
					ret = memec.update( key, valueUpdate, offset );

					if ( ret ) {
						byte[] buffer = value.getBytes();
						byte[] valueUpdateBytes = valueUpdate.getBytes();

						for ( int j = 0; j < length; j++ )
							buffer[ offset + j ] = valueUpdateBytes[ j ];

						value = new String( buffer );
						this.map.put( key, value );
					}

					this.completed[ 2 ]++;
					if ( ret ) this.succeeded[ 2 ]++;
					this.incrementCounter();
					i++;
				} else if ( rand == 3 ) {
					// DELETE
					ret = memec.delete( key );

					if ( ret ) this.map.remove( key );

					// Test whether the key is still available
					if ( memec.get( key ) != null ) {
						System.err.println( "The deleted key (" + key + ") is still available." );
						ret = false;
					}

					this.completed[ 3 ]++;
					if ( ret ) this.succeeded[ 3 ]++;
					this.incrementCounter();
					i++;
				}
			}
		}
	}

	public static void main( String[] args ) throws Exception {
		if ( args.length < 7 ) {
			System.err.println( "Required parameters: [Maximum Key Size] [Chunk Size] [Hostname] [Port Number] [Number of records] [Number of threads] [Number of operations] [Fixed size? (true/false)]" );
			System.exit( 1 );
		}

		try {
			Main.keySize = Integer.parseInt( args[ 0 ] );
			Main.chunkSize = Integer.parseInt( args[ 1 ] );
			Main.host = args[ 2 ];
			Main.port = Integer.parseInt( args[ 3 ] );
			Main.numRecords = Integer.parseInt( args[ 4 ] );
			Main.numThreads = Integer.parseInt( args[ 5 ] );
			Main.numOps = Integer.parseInt( args[ 6 ] );
		} catch( NumberFormatException e ) {
			System.err.println( "Parameters: [Maximum Key Size], [Chunk Size], [Port Number], [Number of records], [Number of threads], and [Number of operations] should be integers." );
			System.exit( 1 );
			return;
		}
		fixedSize = args[ 7 ].equals( "true" );

		/* Initialization */
		Main.mainObjs = new Main[ Main.numThreads ];
		Main.threads = new Thread[ Main.numThreads ];
		Main.lock = new Object();
		for ( int i = 0; i < Main.numThreads; i++ ) {
			int fromId = Integer.MAX_VALUE / Main.numThreads * i;
			int toId = Integer.MAX_VALUE / Main.numThreads * ( i + 1 );
			Main.mainObjs[ i ] = new Main( fromId, toId, i );
			Main.threads[ i ] = new Thread( Main.mainObjs[ i ] );
		}

		/* Start execution */
		for ( int i = 0; i < numThreads; i++ ) {
			Main.threads[ i ].start();
		}

		for ( int i = 0; i < numThreads; i++ ) {
			Main.threads[ i ].join();
		}

		/* Report statistics */
		int[] completed = new int[ 4 ];
		int[] succeeded = new int[ 4 ];
		for ( int i = 0; i < numThreads; i++ ) {
			for ( int j = 0; j < 4; j++ ) {
				completed[ j ] += Main.mainObjs[ i ].completed[ j ];
				succeeded[ j ] += Main.mainObjs[ i ].succeeded[ j ];
			}
		}

		System.out.printf(
			"\n" +
			"Number of SET operations    : %d / %d (failed: %d)\n" +
			"Number of GET operations    : %d / %d (failed: %d)\n" +
			"Number of UPDATE operations : %d / %d (failed: %d)\n" +
			"Number of DELETE operations : %d / %d (failed: %d)\n",
			succeeded[ 0 ], completed[ 0 ], completed[ 0 ] - succeeded[ 0 ],
			succeeded[ 1 ], completed[ 1 ], completed[ 1 ] - succeeded[ 1 ],
			succeeded[ 2 ], completed[ 2 ], completed[ 2 ] - succeeded[ 2 ],
			succeeded[ 3 ], completed[ 3 ], completed[ 3 ] - succeeded[ 3 ]
		);
	}
}
