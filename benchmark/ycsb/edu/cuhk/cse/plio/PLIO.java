package edu.cuhk.cse.plio;

import java.net.Socket;
import java.net.UnknownHostException;

import java.io.BufferedInputStream;
import java.io.OutputStream;
import java.io.IOException;

public class PLIO {
   private Protocol protocol;
   private String host;
   private int port;
   private Socket socket;
   private BufferedInputStream in;
   private OutputStream out;
   private static final boolean isDebugMode = true;

   public PLIO( int keySize, int chunkSize, String host, int port ) {
      this.protocol = new Protocol( keySize, chunkSize );
      this.host = host;
      this.port = port;
   }

   private int read( int size ) throws IOException {
      int recvBytes = 0, ret;
      do {
         ret = this.in.read( this.protocol.buf, recvBytes, size - recvBytes );
         if ( ret > 0 )
            recvBytes += ret;
      } while ( ret >= 0 && recvBytes < size );
      return recvBytes;
   }

   private void debug( String s ) {
      if ( isDebugMode )
         System.out.println( "\n" + s );
   }

   public boolean connect() {
      try {
         this.socket = new Socket( this.host, this.port );
      } catch( UnknownHostException e ) {
         System.err.println( "PLIO.connect(): [Error] Unknown host is specified." );
         System.exit( 1 );
      } catch( IOException e ) {
         System.err.println( "PLIO.connect(): [Error] Fail to connect." );
         System.exit( 1 );
      }

      try {
         this.in = new BufferedInputStream( this.socket.getInputStream() );
      } catch( IOException e ) {
         System.err.println( "PLIO.connect(): [Error] Unable to get socket's input stream." );
         System.exit( 1 );
      }
      try {
         this.out = this.socket.getOutputStream();
      } catch( IOException e ) {
         System.err.println( "PLIO.connect(): [Error] Unable to get socket's output stream." );
         System.exit( 1 );
      }

      // Prepare register message
      int bytes = this.protocol.generateHeader(
         Protocol.PROTO_MAGIC_REQUEST,
         Protocol.PROTO_MAGIC_TO_MASTER,
         Protocol.PROTO_OPCODE_REGISTER,
         0
      );
      try {
         this.out.write( this.protocol.buf, 0, bytes );
      } catch( IOException e ) {
         System.err.println( "PLIO.connect(): [Error] Unable to send register request to master." );
         System.exit( 1 );
      }

      try {
         bytes = this.read( Protocol.PROTO_HEADER_SIZE );
      } catch( IOException e ) {
         System.err.println( "PLIO.connect(): [Warning] Unable to read response from master." );
         return false;
      }
      if ( bytes == Protocol.PROTO_HEADER_SIZE ) {
         this.protocol.parseHeader( bytes );
         this.debug( this.protocol.header );
      }
      return true;
   }

   public boolean disconnect() {
      try {
         this.socket.close();
      } catch( IOException e ) {
         return false;
      }
      return true;
   }

   public boolean get( String key ) {
      return this.get( key.getBytes(), key.length() );
   }

   public boolean set( String key, String value ) {
      return this.set( key.getBytes(), key.length(), value.getBytes(), value.length() );
   }

   public boolean update( String key, String value, int offset ) {
      return this.update( key.getBytes(), key.length(), value.getBytes(), offset, value.length() );
   }

   public boolean delete( String key ) {
      return this.delete( key.getBytes(), key.length() );
   }

   public boolean get( byte[] key, int keySize ) {
      int bytes = this.protocol.generateKeyHeader(
         Protocol.PROTO_MAGIC_REQUEST,
         Protocol.PROTO_MAGIC_TO_MASTER,
         Protocol.PROTO_OPCODE_GET,
         keySize, key
      );
      try {
         this.out.write( this.protocol.buf, 0, bytes );
      } catch( IOException e ) {
         System.err.println( "PLIO.get(): [Warning] Unable to send GET request to master." );
         return false;
      }

      try {
         bytes = this.read( Protocol.PROTO_HEADER_SIZE );
      } catch( IOException e ) {
         System.err.println( "PLIO.get(): [Warning] Unable to read GET response from master." );
         return false;
      }
      if ( bytes == Protocol.PROTO_HEADER_SIZE ) {
         this.protocol.parseHeader( bytes );
         this.debug( this.protocol.header );
      }

      try {
         bytes = this.read( this.protocol.header.length );
      } catch( IOException e ) {
         System.err.println( "PLIO.get(): [Warning] Unable to read GET response from master." );
         return false;
      }
      if ( bytes == this.protocol.header.length ) {
         if ( this.protocol.header.isSuccessful() ) {
            this.protocol.parseKeyValueHeader( bytes, 0 );
            this.debug( this.protocol.keyValueHeader );
         } else {
            this.protocol.parseKeyHeader( bytes, 0 );
            this.debug( this.protocol.keyHeader );
         }
      }
      return true;
   }

   public boolean set( byte[] key, int keySize, byte[] value, int valueSize ) {
      int bytes = this.protocol.generateKeyValueHeader(
         Protocol.PROTO_MAGIC_REQUEST,
         Protocol.PROTO_MAGIC_TO_MASTER,
         Protocol.PROTO_OPCODE_SET,
         keySize, key,
         valueSize, value
      );
      System.out.println( "set() is sending " + bytes + " bytes." );
      try {
         this.out.write( this.protocol.buf, 0, bytes );
      } catch( IOException e ) {
         System.err.println( "PLIO.set(): [Warning] Unable to send SET request to master." );
         return false;
      }

      try {
         bytes = this.read( Protocol.PROTO_HEADER_SIZE );
      } catch( IOException e ) {
         System.err.println( "PLIO.set(): [Warning] Unable to read SET response from master." );
         return false;
      }
      if ( bytes == Protocol.PROTO_HEADER_SIZE ) {
         this.protocol.parseHeader( bytes );
         this.debug( this.protocol.header );
      }

      try {
         bytes = this.read( this.protocol.header.length );
      } catch( IOException e ) {
         System.err.println( "PLIO.set(): [Warning] Unable to read SET response from master." );
         return false;
      }
      if ( bytes == this.protocol.header.length ) {
         this.protocol.parseKeyHeader( bytes, 0 );
         this.debug( this.protocol.keyHeader );
      }
      return true;
   }

   public boolean update( byte[] key, int keySize, byte[] valueUpdate, int valueUpdateOffset, int valueUpdateSize ) {
      int bytes = this.protocol.generateKeyValueUpdateHeader(
         Protocol.PROTO_MAGIC_REQUEST,
         Protocol.PROTO_MAGIC_TO_MASTER,
         Protocol.PROTO_OPCODE_UPDATE,
         keySize, key,
         valueUpdateOffset, valueUpdateSize, valueUpdate
      );
      try {
         this.out.write( this.protocol.buf, 0, bytes );
      } catch( IOException e ) {
         System.err.println( "PLIO.update(): [Warning] Unable to send UPDATE request to master." );
         return false;
      }

      try {
         bytes = this.read( Protocol.PROTO_HEADER_SIZE );
      } catch( IOException e ) {
         System.err.println( "PLIO.update(): [Warning] Unable to read UPDATE response from master." );
         return false;
      }
      if ( bytes == Protocol.PROTO_HEADER_SIZE ) {
         this.protocol.parseHeader( bytes );
         this.debug( this.protocol.header );
      }

      try {
         bytes = this.read( this.protocol.header.length );
      } catch( IOException e ) {
         System.err.println( "PLIO.update(): [Warning] Unable to read UPDATE response from master." );
         return false;
      }
      if ( bytes == this.protocol.header.length ) {
         this.protocol.parseKeyValueUpdateHeader( bytes, 0 );
         this.debug( this.protocol.keyValueUpdateHeader );
      }
      return true;
   }

   public boolean delete( byte[] key, int keySize ) {
      int bytes = this.protocol.generateKeyHeader(
         Protocol.PROTO_MAGIC_REQUEST,
         Protocol.PROTO_MAGIC_TO_MASTER,
         Protocol.PROTO_OPCODE_DELETE,
         keySize, key
      );
      try {
         this.out.write( this.protocol.buf, 0, bytes );
      } catch( IOException e ) {
         System.err.println( "PLIO.delete(): [Warning] Unable to send DELETE request to master." );
         return false;
      }

      try {
         bytes = this.read( Protocol.PROTO_HEADER_SIZE );
      } catch( IOException e ) {
         System.err.println( "PLIO.delete(): [Warning] Unable to read DELETE response from master." );
         return false;
      }
      if ( bytes == Protocol.PROTO_HEADER_SIZE ) {
         this.protocol.parseHeader( bytes );
         this.debug( this.protocol.header );
      }

      try {
         bytes = this.read( this.protocol.header.length );
      } catch( IOException e ) {
         System.err.println( "PLIO.delete(): [Warning] Unable to read DELETE response from master." );
         return false;
      }
      if ( bytes == this.protocol.header.length ) {
         this.protocol.parseKeyHeader( bytes, 0 );
         this.debug( this.protocol.keyHeader );
      }
      return true;
   }
}
