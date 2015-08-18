package edu.cuhk.cse.plio;

public class Protocol {
   public byte[] buf;
   private byte from;

   private static final int PROTO_BUF_MIN_SIZE = 65536;
   public static final byte PROTO_HEADER_SIZE = 8;
   public static final byte PROTO_KEY_VALUE_SIZE = 4;

   /************************
    *  Magic byte (1 byte) *
    ************************/
   // (Bit: 0-2) //
   public static final byte PROTO_MAGIC_HEARTBEAT        = 0x00; // -----000
   public static final byte PROTO_MAGIC_REQUEST          = 0x01; // -----001
   public static final byte PROTO_MAGIC_RESPONSE_SUCCESS = 0x02; // -----010
   public static final byte PROTO_MAGIC_RESPONSE_FAILURE = 0x03; // -----011
   public static final byte PROTO_MAGIC_RESERVED_1       = 0x04; // -----100
   public static final byte PROTO_MAGIC_RESERVED_2       = 0x05; // -----101
   public static final byte PROTO_MAGIC_RESERVED_3       = 0x06; // -----110
   public static final byte PROTO_MAGIC_RESERVED_4       = 0x07; // -----111
   // (Bit: 3-4) //
   public static final byte PROTO_MAGIC_FROM_APPLICATION = 0x00; // ---00---
   public static final byte PROTO_MAGIC_FROM_COORDINATOR = 0x08; // ---01---
   public static final byte PROTO_MAGIC_FROM_MASTER      = 0x10; // ---10---
   public static final byte PROTO_MAGIC_FROM_SLAVE       = 0x18; // ---11---
    // (Bit: 5-6) //
   public static final byte PROTO_MAGIC_TO_APPLICATION   = 0x00; // -00-----
   public static final byte PROTO_MAGIC_TO_COORDINATOR   = 0x20; // -01-----
   public static final byte PROTO_MAGIC_TO_MASTER        = 0x40; // -10-----
   public static final byte PROTO_MAGIC_TO_SLAVE         = 0x60; // -11-----
   // (Bit: 7): Reserved //

   /*******************
    * Opcode (1 byte) *
    *******************/
   // Coordinator-specific opcodes //
   public static final byte PROTO_OPCODE_REGISTER        = 0x00;
   public static final byte PROTO_OPCODE_GET_CONFIG      = 0x09;
   public static final byte PROTO_OPCODE_SYNC            = 0x10;

   // Application <-> Master or Master <-> Slave //
   public static final byte PROTO_OPCODE_GET             = 0x01;
   public static final byte PROTO_OPCODE_SET             = 0x02;
   public static final byte PROTO_OPCODE_UPDATE          = 0x03;
   public static final byte PROTO_OPCODE_DELETE          = 0x04;

   // Slave <-> Slave //
   public static final byte PROTO_OPCODE_UPDATE_CHUNK    = 0x05;
   public static final byte PROTO_OPCODE_DELETE_CHUNK    = 0x06;
   public static final byte PROTO_OPCODE_GET_CHUNK       = 0x07;
   public static final byte PROTO_OPCODE_SET_CHUNK       = 0x08;

   /*********************
    * Key size (1 byte) *
    *********************/
   public static final int MAXIMUM_KEY_SIZE = 255;
   /***********************
    * Value size (3 byte) *
    ***********************/
   public static final int MAXIMUM_VALUE_SIZE = 16777215;

   public static int getSuggestedBufferSize( int keySize, int chunkSize ) {
      int ret = PROTO_HEADER_SIZE + PROTO_KEY_VALUE_SIZE + keySize + chunkSize;
      if ( ( ret & 4095 ) > 0 ) {
         ret >>= 12;
         ret +=1 ;
         ret <<= 12;
      }

      ret <<= 1;
      if ( ret < PROTO_BUF_MIN_SIZE )
         ret = PROTO_BUF_MIN_SIZE;

      return ret;
   }

   public Protocol( int keySize, int chunkSize ) {
      this.buf = new byte[ Protocol.getSuggestedBufferSize( keySize, chunkSize ) ];
      this.from = PROTO_MAGIC_FROM_APPLICATION;
   }

   public int generateHeader( byte magic, byte to, byte opcode, int length ) {
      this.buf[ 0 ]  = ( byte )( magic & 0x07 );
      this.buf[ 0 ] |= ( byte )( this.from & 0x18 );
      this.buf[ 0 ] |= ( byte )( to & 0x60 );

      this.buf[ 1 ] = ( byte )( opcode & 0xFF );
   	this.buf[ 2 ] = 0;
   	this.buf[ 3 ] = 0;
      this.buf[ 4 ] = ( byte )( length >> 24 );
      this.buf[ 5 ] = ( byte )( length >> 16 );
      this.buf[ 6 ] = ( byte )( length >>  8 );
      this.buf[ 7 ] = ( byte )( length       );
      return PROTO_HEADER_SIZE;
   }

   public boolean parseHeader( int size ) {
      byte magic, from, to, opcode;
      int length;

      if ( size < PROTO_HEADER_SIZE )
         return false;

      magic  = ( byte )( this.buf[ 0 ] & 0x07 );
      from   = ( byte )( this.buf[ 0 ] & 0x18 );
      to     = ( byte )( this.buf[ 0 ] & 0x60 );
      opcode = ( byte )( this.buf[ 1 ] & 0xFF );
      length = ( this.buf[ 4 ] << 24 ) |
               ( this.buf[ 5 ] << 16 ) |
               ( this.buf[ 6 ] <<  8 ) |
               ( this.buf[ 7 ]       );

      System.err.print( "(magic, from, to, opcode, length) = \n\t(" );

      switch( magic ) {
         case PROTO_MAGIC_HEARTBEAT:
            System.err.print( "PROTO_MAGIC_HEARTBEAT, " );
            break;
   		case PROTO_MAGIC_REQUEST:
            System.err.print( "PROTO_MAGIC_REQUEST, " );
            break;
   		case PROTO_MAGIC_RESPONSE_SUCCESS:
            System.err.print( "PROTO_MAGIC_RESPONSE_SUCCESS, " );
            break;
   		case PROTO_MAGIC_RESPONSE_FAILURE:
            System.err.print( "PROTO_MAGIC_RESPONSE_FAILURE, " );
   			break;
   		default:
   			return false;
      }

      switch( from ) {
         case PROTO_MAGIC_FROM_APPLICATION:
            System.err.print( "PROTO_MAGIC_FROM_APPLICATION, " );
            break;
   		case PROTO_MAGIC_FROM_COORDINATOR:
            System.err.print( "PROTO_MAGIC_FROM_COORDINATOR, " );
            break;
   		case PROTO_MAGIC_FROM_MASTER:
            System.err.print( "PROTO_MAGIC_FROM_MASTER, " );
            break;
   		case PROTO_MAGIC_FROM_SLAVE:
            System.err.print( "PROTO_MAGIC_FROM_SLAVE, " );
            break;
         default:
            return false;
      }

      switch( to ) {
         case PROTO_MAGIC_TO_APPLICATION:
            System.err.print( "PROTO_MAGIC_TO_APPLICATION, " );
            break;
   		case PROTO_MAGIC_TO_COORDINATOR:
            System.err.print( "PROTO_MAGIC_TO_COORDINATOR, " );
            break;
   		case PROTO_MAGIC_TO_MASTER:
            System.err.print( "PROTO_MAGIC_TO_MASTER, " );
            break;
   		case PROTO_MAGIC_TO_SLAVE:
            System.err.print( "PROTO_MAGIC_TO_SLAVE, " );
            break;
         default:
            return false;
      }

      switch( opcode ) {
         case PROTO_OPCODE_REGISTER:
            System.err.print( "PROTO_OPCODE_REGISTER" );
            break;
   		case PROTO_OPCODE_GET_CONFIG:
            System.err.print( "PROTO_OPCODE_GET_CONFIG" );
            break;
   		case PROTO_OPCODE_SYNC:
            System.err.print( "PROTO_OPCODE_SYNC" );
            break;
   		case PROTO_OPCODE_GET:
            System.err.print( "PROTO_OPCODE_GET" );
            break;
   		case PROTO_OPCODE_SET:
            System.err.print( "PROTO_OPCODE_SET" );
            break;
   		case PROTO_OPCODE_UPDATE:
            System.err.print( "PROTO_OPCODE_UPDATE" );
            break;
   		case PROTO_OPCODE_DELETE:
            System.err.print( "PROTO_OPCODE_DELETE" );
            break;
   		case PROTO_OPCODE_UPDATE_CHUNK:
            System.err.print( "PROTO_OPCODE_UPDATE_CHUNK" );
            break;
   		case PROTO_OPCODE_DELETE_CHUNK:
            System.err.print( "PROTO_OPCODE_DELETE_CHUNK" );
            break;
   		case PROTO_OPCODE_GET_CHUNK:
            System.err.print( "PROTO_OPCODE_GET_CHUNK" );
            break;
   		case PROTO_OPCODE_SET_CHUNK:
            System.err.print( "PROTO_OPCODE_SET_CHUNK" );
            break;
   		default:
   			return false;
      }

      System.err.println( ", " + length + ")" );

      return true;
   }
}
