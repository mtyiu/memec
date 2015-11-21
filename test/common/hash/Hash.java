public class Hash {
	public static long hash( byte[] data ) {
		int hash = 388650013;
		int scale = 388650179;
		int hardener  = 1176845762;
		int i = 0, n = data.length;
		while ( n > 0 ) {
			hash = ( hash * scale ) & 0xFFFFFFFF;
			hash = ( hash + data[ i++ ] ) & 0xFFFFFFFF;
			n--;
		}
		return ( ( hash ^ hardener ) & ( -1L >>> 32 ) );
	}

	public static void main( String[] args ) {
		if ( args.length != 1 ) {
			System.err.println( "Usage: java Hash [input string]" );
			return;
		}

		byte[] input = args[ 0 ].getBytes();

		System.out.println( Hash.hash( input ) );
	}
}
