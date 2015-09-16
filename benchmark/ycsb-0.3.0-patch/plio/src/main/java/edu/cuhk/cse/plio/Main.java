package edu.cuhk.cse.plio;

import java.util.Scanner;

public class Main {
	public static void help( boolean fileMode ) {
		if ( fileMode ) {
			System.out.println(
				"Supported commands:\n"
				+ "- help								: Show this help message\n"
				+ "- exit								: Terminate this client\n"
				+ "- set [key] [src]				 : Upload the file at [src] with key [key]\n"
				+ "- get [key] [dest]				: Download the file with key [key] to the destination [dest]\n"
				+ "- update [key] [src] [offset] : Update the data at [offset] with the contents in [src]\n"
				+ "- delete [key]					 : Delete the key [key]\n"
			);
		} else {
			System.out.println(
				"Supported commands:\n"
				+ "- help								  : Show this help message\n"
				+ "- exit								  : Terminate this client\n"
				+ "- set [key] [value]				 : Set a new value [value] with key [key]\n"
				+ "- get [key]							: Get the value with key [key]\n"
				+ "- update [key] [value] [offset] : Update the data at [offset] with the contents in [value]\n"
				+ "- delete [key]						: Delete the key [key]\n"
			);
		}

	}

	public static void main( String[] args ) throws Exception {
		if ( args.length < 4 ) {
			System.err.println( "Required parameters: [Key Size] [Chunk Size] [Hostname] [Port Number] [Mode (file / console)]" );
			System.exit( 1 );
		}

		int keySize = 0, chunkSize = 0, port = 0;
		String host = "";
		boolean fileMode = true;
		PLIO plio;

		try {
			keySize = Integer.parseInt( args[ 0 ] );
			chunkSize = Integer.parseInt( args[ 1 ] );
			host = args[ 2 ];
			port = Integer.parseInt( args[ 3 ] );
		} catch( NumberFormatException e ) {
			System.err.println( "Both parameters: [Key Size], [Chunk Size] & [Port Number] should be integers." );
			System.exit( 1 );
		}
		if ( args.length > 4 )
			fileMode = ! args[ 4 ].equals( "console" );

		plio = new PLIO( keySize, chunkSize, host, port );
		boolean run = plio.connect();

		Scanner scanner = new Scanner( System.in );
		String input;
		String[] tokens;

		Main.help( fileMode );
		while( run ) {
			System.out.print( "> " );
			input = scanner.nextLine();
			input.trim();
			if ( input.length() == 0 ) continue;
			tokens = input.split( "\\s" );

			// Determine action
			//////////////////////////////////////////////////////////////////////
			if ( tokens[ 0 ].equals( "set" ) ) {
				if ( tokens.length != 3 ) {
					System.err.println( "Invalid SET command!" );
					continue;
				}
				System.err.printf( "[SET] {%s} %s\n", tokens[ 1 ], tokens[ 2 ] );
				if ( fileMode ) {
				} else {
					plio.set(
						tokens[ 1 ].getBytes(),
						tokens[ 1 ].getBytes().length,
						tokens[ 2 ].getBytes(),
						tokens[ 2 ].getBytes().length
					);
				}
			//////////////////////////////////////////////////////////////////////
			} else if ( tokens[ 0 ].equals( "get" ) ) {
				if ( fileMode ) {
					if ( tokens.length != 3 ) {
						System.err.println( "Invalid GET command!" );
						continue;
					}
					System.err.printf( "[GET] {%s} %s\n", tokens[ 1 ], tokens[ 2 ] );
					plio.get(
						tokens[ 1 ].getBytes(),
						tokens[ 1 ].getBytes().length
					);
				} else {
					if ( tokens.length != 2 ) {
						System.err.println( "Invalid GET command!" );
						continue;
					}
					System.err.printf( "[GET] {%s}\n", tokens[ 1 ] );
					plio.get(
						tokens[ 1 ].getBytes(),
						tokens[ 1 ].getBytes().length
					);
				}

			//////////////////////////////////////////////////////////////////////
			} else if ( tokens[ 0 ].equals( "update" ) ) {
				if ( tokens.length != 4 ) {
					System.err.println( "Invalid UPDATE command!" );
					continue;
				}
				int offset;
				try {
					offset = Integer.parseInt( tokens[ 3 ] );
				} catch( NumberFormatException e ) {
					System.err.println( "Invalid UPDATE command!" );
					continue;
				}
				System.err.printf( "[UPDATE] {%s} %s at offset %s\n", tokens[ 1 ], tokens[ 2 ], tokens[ 3 ] );
				if ( fileMode ) {
				} else {
					plio.update(
						tokens[ 1 ].getBytes(),
						tokens[ 1 ].getBytes().length,
						tokens[ 2 ].getBytes(),
						offset,
						tokens[ 2 ].getBytes().length
					);
				}
			//////////////////////////////////////////////////////////////////////
			} else if ( tokens[ 0 ].equals( "delete" ) ) {
				if ( tokens.length != 2 ) {
					System.err.println( "Invalid DELETE command!" );
					continue;
				}
				System.err.printf( "[DELETE] {%s}\n", tokens[ 1 ] );
				plio.delete(
					tokens[ 1 ].getBytes(),
					tokens[ 1 ].getBytes().length
				);
			//////////////////////////////////////////////////////////////////////
			} else if ( tokens[ 0 ].equals( "help" ) ) {
				Main.help( fileMode );
			} else if ( tokens[ 0 ].equals( "exit" ) || tokens[ 0 ].equals( "quit" ) ) {
				run = false;
			//////////////////////////////////////////////////////////////////////
			} else {
				System.err.println( "Invalid command!" );
			}
		}
		System.err.println( "\nBye!" );
	}
}
