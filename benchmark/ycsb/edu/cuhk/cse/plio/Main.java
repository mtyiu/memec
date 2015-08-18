package edu.cuhk.cse.plio;

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
         System.err.println( "Both parameters: [Key Size] [Chunk Size] should be integers." );
         System.exit( 1 );
      }

      plio = new PLIO( keySize, chunkSize, host, port );
      plio.connect();
   }
}
