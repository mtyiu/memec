package edu.cuhk.cse.plio;

import java.util.Scanner;

public class Main {
   public static void main( String[] args ) throws Exception {
      if ( args.length < 4 ) {
         System.err.println( "Required parameters: [Key Size] [Chunk Size] [Hostname] [Port Number]" );
         System.exit( 1 );
      }

      int keySize = 0, chunkSize = 0, port = 0;
      String host = "";
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

      plio = new PLIO( keySize, chunkSize, host, port );
      boolean run = plio.connect();

      Scanner scanner = new Scanner( System.in );
      String input;
      String[] tokens;

      while( run ) {
         System.out.print( "> " );
         input = scanner.nextLine();
         input.trim();
         if ( input.length() == 0 ) continue;
         tokens = input.split( "\\s" );

         // Determine action
         if ( tokens[ 0 ].equals( "set" ) ) {
            System.err.println( "[SET]" );
         } else if ( tokens[ 0 ].equals( "get" ) ) {
            System.err.println( "[GET]" );
         } else if ( tokens[ 0 ].equals( "update" ) ) {
            System.err.println( "[UPDATE]" );
         } else if ( tokens[ 0 ].equals( "delete" ) ) {
            System.err.println( "[DELETE]" );
         } else if ( tokens[ 0 ].equals( "exit" ) || tokens[ 0 ].equals( "quit" ) ) {
            run = false;
         } else {
            System.err.println( "Invalid command!" );
         }
      }
      System.err.println( "\nBye!" );
   }
}
