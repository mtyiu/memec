package edu.cuhk.cse.plio;

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
	public static int numThreads;
	public static long totalSize;
	public static boolean fixedSize = true;
	/* States */
	public static Main[] mainObjs;
	public static Thread[] threads;
	public static long bytes;
	public static Object lock;
	/* Constants */
	private static final String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	private static final int charactersLength = characters.length();

	/* Instance variables */
	private PLIO plio;
	private Random random;
	public int completed, succeeded;

	public Main( int fromId, int toId ) {
		this.plio = new PLIO( Main.keySize, Main.chunkSize, Main.host, Main.port, fromId, toId );
		this.completed = 0;
		this.succeeded = 0;
		this.random = new Random();
	}

	private String generate( int size, boolean toLower ) {
		StringBuilder sb = new StringBuilder( size );
		for ( int i = 0; i < size; i++ ) {
			sb.append( Main.characters.charAt( this.random.nextInt( Main.charactersLength ) ) );
		}
		String ret = sb.toString();
		return toLower ? ret.toLowerCase() : ret;
	}

	private long increment( int bytes ) {
		long ret;
		synchronized( Main.lock ) {
			Main.bytes += bytes;
			ret = Main.bytes;
		}
		System.out.printf( "Written bytes: %ld / %ld\r", ret, Main.totalSize );
		return ret;
	}

	public void run() {
		if ( ! plio.connect() )
			return;

		int rand, size;

		int i = 0, keySize, valueSize, index;
		long numBytes = Main.totalSize / Main.numThreads, bytes = 0;
		String key, value;
		boolean ret;

		while( bytes < numBytes ) {
			ret = false;

			// Construct new key-value pair
			keySize = Main.keySize >> 3;
			valueSize = Main.fixedSize ? ( Main.chunkSize >> 5 ) : this.random.nextInt( Main.chunkSize >> 3 );
			if ( valueSize < 4 ) valueSize = 4;

			// Store it in the HashMap
			key = this.generate( keySize, false );
			value = this.generate( valueSize, true );

			// Issue the request
			ret = plio.set( key, value );

			this.completed++;
			if ( ret ) this.succeeded++;
			this.increment( keySize + valueSize );
			bytes += keySize + valueSize;
			i++;
		}
	}

	public static void main( String[] args ) throws Exception {
		if ( args.length < 6 ) {
			System.err.println( "Required parameters: [Maximum Key Size] [Chunk Size] [Hostname] [Port Number] [Total Size] [Number of threads] [Fixed size? (true/false)]" );
			System.exit( 1 );
		}

		try {
			Main.keySize = Integer.parseInt( args[ 0 ] );
			Main.chunkSize = Integer.parseInt( args[ 1 ] );
			Main.host = args[ 2 ];
			Main.port = Integer.parseInt( args[ 3 ] );
			Main.totalSize = Long.parseLong( args[ 4 ] );
			Main.numThreads = Integer.parseInt( args[ 5 ] );
		} catch( NumberFormatException e ) {
			System.err.println( "Parameters: [Maximum Key Size], [Chunk Size], [Port Number], [Total Size] and [Number of threads] should be integers." );
			System.exit( 1 );
			return;
		}
		fixedSize = args[ 6 ].equals( "true" );

		/* Initialization */
		Main.mainObjs = new Main[ Main.numThreads ];
		Main.threads = new Thread[ Main.numThreads ];
		Main.lock = new Object();
		for ( int i = 0; i < Main.numThreads; i++ ) {
			int fromId = Integer.MAX_VALUE / Main.numThreads * i;
			int toId = Integer.MAX_VALUE / Main.numThreads * ( i + 1 );
			Main.mainObjs[ i ] = new Main( fromId, toId );
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
		int completed = 0;
		int succeeded = 0;
		for ( int i = 0; i < numThreads; i++ ) {
			completed += Main.mainObjs[ i ].completed;
			succeeded += Main.mainObjs[ i ].succeeded;
		}

		System.out.println();
	}
}
