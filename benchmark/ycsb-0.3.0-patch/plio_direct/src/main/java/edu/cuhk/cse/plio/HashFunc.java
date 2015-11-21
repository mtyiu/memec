package edu.cuhk.cse.plio;

public class HashFunc {
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
}
