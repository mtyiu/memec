package edu.cuhk.cse.plio;

import java.net.Socket;
import java.net.UnknownHostException;

import java.io.BufferedInputStream;
import java.io.OutputStream;
import java.io.IOException;

public class PLIODirect {
	private ProtocolDirect protocol;
	private int count;
	private String[] hosts;
	private int[] ports;
	private int id;
	private int fromId;
	private int toId;
	private Socket[] sockets;
	private BufferedInputStream[] ins;
	private OutputStream[] outs;
	private static final boolean isDebugMode = true;

	public static int DEFAULT_PORT = 9110;
	public static int DEFAULT_KEY_SIZE = 255;
	public static int DEFAULT_CHUNK_SIZE = 4096;

	public PLIODirect( int keySize, int chunkSize, String[] hosts, int[] ports, int fromId, int toId ) {
		this.protocol = new ProtocolDirect( keySize, chunkSize );
		this.count = hosts.length;
		this.hosts = hosts;
		this.ports = ports;
		this.id = fromId;
		this.fromId = fromId;
		this.toId = toId;
		this.sockets = new Socket[ this.count ];
		this.ins = new BufferedInputStream[ this.count ];
		this.outs = new OutputStream[ this.count ];
	}

	public int nextVal() {
		if ( this.id == this.toId - 1 )
			this.id = this.fromId;
		else
			this.id++;
		return this.id;
	}

	private int read( int i, int size ) throws IOException {
		int recvBytes = 0, ret;
		do {
			ret = this.ins[ i ].read( this.protocol.buf, recvBytes, size - recvBytes );
			if ( ret > 0 )
				recvBytes += ret;
		} while ( ret >= 0 && recvBytes < size );
		return recvBytes;
	}

	private int hash( byte[] key ) {
		return ( int )( HashFunc.hash( key ) % this.count );
	}

	private void debug( String s ) {
		if ( isDebugMode )
			System.out.println( "\n" + s );
	}

	public boolean connect() {
		boolean ret = true;
		for ( int i = 0; i < this.count; i++ ) {
			try {
				this.sockets[ i ] = new Socket( this.hosts[ i ], this.ports[ i ] );
			} catch( UnknownHostException e ) {
				System.err.println( "PLIO.connect(): [Error] Unknown host is specified." );
				System.exit( 1 );
			} catch( IOException e ) {
				System.err.println( "PLIO.connect(): [Error] Fail to connect." );
				System.exit( 1 );
			}

			try {
				this.ins[ i ] = new BufferedInputStream( this.sockets[ i ].getInputStream() );
			} catch( IOException e ) {
				System.err.println( "PLIO.connect(): [Error] Unable to get socket's input stream." );
				System.exit( 1 );
			}
			try {
				this.outs[ i ] = this.sockets[ i ].getOutputStream();
			} catch( IOException e ) {
				System.err.println( "PLIO.connect(): [Error] Unable to get socket's output stream." );
				System.exit( 1 );
			}

			// Prepare register message
			int id = this.nextVal();
			int bytes = this.protocol.generateAddressHeader(
				ProtocolDirect.PROTO_MAGIC_REQUEST,
				ProtocolDirect.PROTO_MAGIC_TO_SLAVE,
				ProtocolDirect.PROTO_OPCODE_REGISTER,
				id,
				0, // My IP address
				( short ) 0  // My port number
			);
			try {
				this.outs[ i ].write( this.protocol.buf, 0, bytes );
			} catch( IOException e ) {
				System.err.println( "PLIO.connect(): [Error] Unable to send register request to master." );
				System.exit( 1 );
			}

			try {
				bytes = this.read( i, ProtocolDirect.PROTO_HEADER_SIZE );
			} catch( IOException e ) {
				System.err.println( "PLIO.connect(): [Error] Unable to read response from master." );
				ret = false;
			}
			if ( bytes == ProtocolDirect.PROTO_HEADER_SIZE ) {
				this.protocol.parseHeader( bytes );
				if ( this.protocol.header.id != id ) {
					System.err.println( "PLIO.connect(): [Error] The response does not match the request ID." );
					ret = false;
				}
				// this.debug( this.protocol.header.toString() );
			} else {
				System.err.println( "PLIO.connect(): [Error] Header length mismatch: " + bytes + " vs. " + this.protocol.header.length + "." );
				ret = false;
			}
		}
		return ret;
	}

	public boolean disconnect() {
		boolean ret = true;
		for ( int i = 0; i < this.count; i++ ) {
			try {
				this.sockets[ i ].close();
			} catch( IOException e ) {
				ret = false;
			}
		}
		return true;
	}

	public String get( String key ) {
		byte[] k = key.getBytes();
		return this.get( k, k.length );
	}

	public boolean set( String key, String value ) {
		byte[] k = key.getBytes();
		byte[] v = value.getBytes();
		return this.set( k, k.length, v, v.length );
	}

	public boolean update( String key, String value, int offset ) {
		byte[] k = key.getBytes();
		byte[] v = value.getBytes();
		return this.update( k, k.length, v, offset, v.length );
	}

	public boolean delete( String key ) {
		byte[] k = key.getBytes();
		return this.delete( k, k.length );
	}

	public String get( byte[] key, int keySize ) {
		int id = this.nextVal();
		int bytes = this.protocol.generateKeyHeader(
			ProtocolDirect.PROTO_MAGIC_REQUEST,
			ProtocolDirect.PROTO_MAGIC_TO_SLAVE,
			ProtocolDirect.PROTO_OPCODE_GET,
			id,
			keySize, key
		);
		int index = this.hash( key );
		try {
			this.outs[ index ].write( this.protocol.buf, 0, bytes );
		} catch( IOException e ) {
			System.err.println( "PLIO.get(): [Error] Unable to send GET request to master." );
			return null;
		}

		try {
			bytes = this.read( index, ProtocolDirect.PROTO_HEADER_SIZE );
		} catch( IOException e ) {
			System.err.println( "PLIO.get(): [Error] Unable to read GET response from master." );
			return null;
		}
		if ( bytes == ProtocolDirect.PROTO_HEADER_SIZE ) {
			this.protocol.parseHeader( bytes );
			// this.debug( this.protocol.header.toString() );
		}

		try {
			bytes = this.read( index, this.protocol.header.length );
		} catch( IOException e ) {
			System.err.println( "PLIO.get(): [Error] Unable to read GET response from master." );
			return null;
		}
		if ( bytes == this.protocol.header.length ) {
			if ( this.protocol.header.id != id ) {
				System.err.println( "PLIO.get(): [Error] The response does not match the request ID." );
			} else if ( this.protocol.header.isSuccessful() ) {
				this.protocol.parseKeyValueHeader( bytes, 0 );
				// this.debug( this.protocol.keyValueHeader.toString() );
				if ( this.protocol.keyValueHeader.match( key, keySize ) )
					return this.protocol.keyValueHeader.value();
				System.err.println( "PLIO.get(): [Error] The response does not match with the key." );
			} else {
				this.protocol.parseKeyHeader( bytes, 0 );
				// this.debug( this.protocol.keyHeader.toString() );
				System.err.println( "PLIO.get(): [Error] Key not found." );
			}
		} else {
			System.err.println( "PLIO.get(): [Error] Header length mismatch: " + bytes + " vs. " + this.protocol.header.length + "." );
		}
		return null;
	}

	public boolean set( byte[] key, int keySize, byte[] value, int valueSize ) {
		int id = this.nextVal();
		int bytes = this.protocol.generateKeyValueHeader(
			ProtocolDirect.PROTO_MAGIC_REQUEST,
			ProtocolDirect.PROTO_MAGIC_TO_SLAVE,
			ProtocolDirect.PROTO_OPCODE_SET,
			id,
			keySize, key,
			valueSize, value
		);
		int index = this.hash( key );
		try {
			this.outs[ index ].write( this.protocol.buf, 0, bytes );
		} catch( IOException e ) {
			System.err.println( "PLIO.set(): [Error] Unable to send SET request to master." );
			return false;
		}

		try {
			bytes = this.read( index, ProtocolDirect.PROTO_HEADER_SIZE );
		} catch( IOException e ) {
			System.err.println( "PLIO.set(): [Error] Unable to read SET response from master." );
			return false;
		}
		if ( bytes == ProtocolDirect.PROTO_HEADER_SIZE ) {
			this.protocol.parseHeader( bytes );
			// this.debug( this.protocol.header.toString() );
		}

		try {
			bytes = this.read( index, this.protocol.header.length );
		} catch( IOException e ) {
			System.err.println( "PLIO.set(): [Error] Unable to read SET response from master." );
			return false;
		}
		if ( bytes == this.protocol.header.length ) {
			if ( this.protocol.header.id != id ) {
				System.err.println( "PLIO.set(): [Error] The response does not match the request ID." );
			} else if ( this.protocol.header.isSuccessful() ) {
				this.protocol.parseKeyHeader( bytes, 0 );
				// this.debug( this.protocol.keyHeader.toString() );
				if ( this.protocol.keyHeader.match( key, keySize ) ) {
					return true;
				} else {
					System.err.println( "PLIO.set(): [Error] The response does not match with the key." );
				}
			} else {
				System.err.println( "PLIO.set(): [Error] Request failed." );
			}
		} else {
			System.err.println( "PLIO.set(): [Error] Header length mismatch: " + bytes + " vs. " + this.protocol.header.length + "." );
		}
		return false;
	}

	public boolean update( byte[] key, int keySize, byte[] valueUpdate, int valueUpdateOffset, int valueUpdateSize ) {
		int id = this.nextVal();
		int bytes = this.protocol.generateKeyValueUpdateHeader(
			ProtocolDirect.PROTO_MAGIC_REQUEST,
			ProtocolDirect.PROTO_MAGIC_TO_SLAVE,
			ProtocolDirect.PROTO_OPCODE_UPDATE,
			id,
			keySize, key,
			valueUpdateOffset, valueUpdateSize, valueUpdate
		);
		int index = this.hash( key );
		try {
			this.outs[ index ].write( this.protocol.buf, 0, bytes );
		} catch( IOException e ) {
			System.err.println( "PLIO.update(): [Error] Unable to send UPDATE request to master." );
			return false;
		}

		try {
			bytes = this.read( index, ProtocolDirect.PROTO_HEADER_SIZE );
		} catch( IOException e ) {
			System.err.println( "PLIO.update(): [Error] Unable to read UPDATE response from master." );
			return false;
		}
		if ( bytes == ProtocolDirect.PROTO_HEADER_SIZE ) {
			this.protocol.parseHeader( bytes );
			// this.debug( this.protocol.header.toString() );
		}

		try {
			bytes = this.read( index, this.protocol.header.length );
		} catch( IOException e ) {
			System.err.println( "PLIO.update(): [Error] Unable to read UPDATE response from master." );
			return false;
		}
		if ( bytes == this.protocol.header.length ) {
			if ( this.protocol.header.id != id ) {
				System.err.println( "PLIO.update(): [Error] The response does not match the request ID." );
			} else if ( this.protocol.header.isSuccessful() ) {
				this.protocol.parseKeyValueUpdateHeader( bytes, 0 );
				// this.debug( this.protocol.keyValueUpdateHeader.toString() );
				if ( this.protocol.keyValueUpdateHeader.match( key, keySize ) )
					return true;
				else
					System.err.println( "PLIO.update(): [Error] The response does not match with the key." );
			} else {
				System.err.println( "PLIO.update(): [Error] Request failed." );
			}
		} else {
			System.err.println( "PLIO.update(): [Error] Header length mismatch: " + bytes + " vs. " + this.protocol.header.length + "." );
		}
		return false;
	}

	public boolean delete( byte[] key, int keySize ) {
		int id = this.nextVal();
		int bytes = this.protocol.generateKeyHeader(
			ProtocolDirect.PROTO_MAGIC_REQUEST,
			ProtocolDirect.PROTO_MAGIC_TO_SLAVE,
			ProtocolDirect.PROTO_OPCODE_DELETE,
			id,
			keySize, key
		);
		int index = this.hash( key );
		try {
			this.outs[ index ].write( this.protocol.buf, 0, bytes );
		} catch( IOException e ) {
			System.err.println( "PLIO.delete(): [Error] Unable to send DELETE request to master." );
			return false;
		}

		try {
			bytes = this.read( index, ProtocolDirect.PROTO_HEADER_SIZE );
		} catch( IOException e ) {
			System.err.println( "PLIO.delete(): [Error] Unable to read DELETE response from master." );
			return false;
		}
		if ( bytes == ProtocolDirect.PROTO_HEADER_SIZE ) {
			this.protocol.parseHeader( bytes );
			// this.debug( this.protocol.header.toString() );
		}

		try {
			bytes = this.read( index, this.protocol.header.length );
		} catch( IOException e ) {
			System.err.println( "PLIO.delete(): [Error] Unable to read DELETE response from master." );
			return false;
		}
		if ( bytes == this.protocol.header.length ) {
			if ( this.protocol.header.id != id ) {
				System.err.println( "PLIO.delete(): [Error] The response does not match the request ID." );
			} else if ( this.protocol.header.isSuccessful() ) {
				this.protocol.parseKeyHeader( bytes, 0 );
				// this.debug( this.protocol.keyHeader.toString() );
				if ( this.protocol.keyHeader.match( key, keySize ) ) {
					return true;
				} else {
					System.err.println( "PLIO.delete(): [Error] The response does not match with the key." );
				}
			} else {
				System.err.println( "PLIO.delete(): [Error] Request failed." );
			}
		} else {
			System.err.println( "PLIO.delete(): [Error] Header length mismatch: " + bytes + " vs. " + this.protocol.header.length + "." );
		}
		return false;
	}
}
