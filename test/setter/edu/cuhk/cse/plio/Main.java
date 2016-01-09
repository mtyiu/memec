package edu.cuhk.cse.plio;

import java.util.Scanner;
import java.util.HashMap;
import java.util.Random;
import java.util.Map;
import java.util.Set;

public class Main implements Runnable {
	/* Configuration */
	public static int maxKeySize, chunkSize, port;
	public static String host;
	/* Test parameters */
	public static int numThreads;
	public static long totalSize;
	public static int keySize;
	public static int valueSize;
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
		this.plio = new PLIO( Main.maxKeySize, Main.chunkSize, Main.host, Main.port, fromId, toId );
		this.completed = 0;
		this.succeeded = 0;
		this.random = new Random();
	}

	private void generate( byte[] buf, int size, boolean toLower ) {
		for ( int i = 0; i < size; i++ ) {
			buf[ i ] = ( byte ) Main.characters.charAt( this.random.nextInt( Main.charactersLength ) );

			if ( toLower )
				buf[ i ] = ( byte ) Character.toLowerCase( buf[ i ] );
		}
	}

	private long increment( long bytes ) {
		long ret;
		synchronized( Main.lock ) {
			Main.bytes += bytes;
			ret = Main.bytes;
		}
		return ret;
	}

	public void run() {
		if ( ! plio.connect() )
			return;

		int rand, size;

		int i = 0, index;
		long numBytes = Main.totalSize / Main.numThreads, bytes = 0;
		byte[] key = new byte[ Main.keySize ];
		byte[] value = new byte[ Main.valueSize ];
		boolean ret;

		while( bytes < numBytes ) {
			ret = false;

			// Store it in the HashMap
			this.generate( key, Main.keySize, false );
			this.generate( value, Main.valueSize, true );

			// Issue the request
			ret = plio.set( key, Main.keySize, value, Main.valueSize );

			this.completed++;
			if ( ret ) this.succeeded++;

			bytes += keySize + valueSize;
			i++;
		}

		this.increment( bytes );
	}

	public static void main( String[] args ) throws Exception {
		if ( args.length < 8 ) {
			System.err.println( "Required parameters: [Maximum Key Size] [Chunk Size] [Hostname] [Port Number] [Total Size] [Key Size] [Value Size] [Number of threads]" );
			System.exit( 1 );
		}

		try {
			Main.maxKeySize = Integer.parseInt( args[ 0 ] );
			Main.chunkSize = Integer.parseInt( args[ 1 ] );
			Main.host = args[ 2 ];
			Main.port = Integer.parseInt( args[ 3 ] );
			Main.totalSize = Long.parseLong( args[ 4 ] );
			Main.keySize = Integer.parseInt( args[ 5 ] );
			Main.valueSize = Integer.parseInt( args[ 6 ] );
			Main.numThreads = Integer.parseInt( args[ 7 ] );
		} catch( NumberFormatException e ) {
			System.err.println( "Parameters: [Maximum Key Size], [Chunk Size], [Port Number], [Total Size] and [Number of threads] should be integers." );
			System.exit( 1 );
			return;
		}

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
