package edu.cuhk.cse.plio;

import java.util.Scanner;

public class Main implements Runnable {
	private PLIO plio;

	public Main( int keySize, int chunkSize, String host, int port, int startId ) {
		plio = new PLIO( keySize, chunkSize, host, port, startId );
		System.out.println( "Start ID: " + startId );
	}

	public void run() {
		if ( ! plio.connect() )
			return;

		System.out.println( "Hello World!" );
	}

	public static void main( String[] args ) throws Exception {
		if ( args.length < 7 ) {
			System.err.println( "Required parameters: [Maximum Key Size] [Chunk Size] [Hostname] [Port Number] [Number of records] [Number of threads] [Number of operations] [Fixed size? (true/false)]" );
			System.exit( 1 );
		}

		/* Configuration */
		int keySize = 0, chunkSize = 0, port = 0;
		String host = "";
		/* Test parameters */
		int numThreads, numRecords, numOps;
		boolean fixedSize = true;
		/* States */
		Main[] mainObjs;
		Thread[] threads;

		try {
			keySize = Integer.parseInt( args[ 0 ] );
			chunkSize = Integer.parseInt( args[ 1 ] );
			host = args[ 2 ];
			port = Integer.parseInt( args[ 3 ] );
			numRecords = Integer.parseInt( args[ 4 ] );
			numThreads = Integer.parseInt( args[ 5 ] );
			numOps = Integer.parseInt( args[ 6 ] );
		} catch( NumberFormatException e ) {
			System.err.println( "Both parameters: [Key Size], [Chunk Size] & [Port Number] should be integers." );
			System.exit( 1 );
			return;
		}
		fixedSize = ! args[ 7 ].equals( "true" );

		/* Initialization */
		mainObjs = new Main[ numThreads ];
		threads = new Thread[ numThreads ];
		for ( int i = 0; i < numThreads; i++ ) {
			int startId = Integer.MAX_VALUE / numThreads * i;
			mainObjs[ i ] = new Main( keySize, chunkSize, host, port, startId );
			threads[ i ] = new Thread( mainObjs[ i ] );
		}

		for ( int i = 0; i < numThreads; i++ ) {
			threads[ i ].start();
		}
	}
}
