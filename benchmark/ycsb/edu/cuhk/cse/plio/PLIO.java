package edu.cuhk.cse.plio;

import java.net.Socket;
import java.net.UnknownHostException;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

public class PLIO {
   private Protocol protocol;
   private String host;
   private int port;
   private Socket socket;
   private InputStream in;
   private OutputStream out;

   public PLIO( int keySize, int chunkSize, String host, int port ) {
      this.protocol = new Protocol( keySize, chunkSize );
      this.host = host;
      this.port = port;
   }

   private int read( int size ) throws Exception {
      int recvBytes = 0, ret;
      do {
         ret = this.in.read( this.protocol.buf, recvBytes, size - recvBytes );
         if ( ret > 0 )
            recvBytes += ret;
      } while ( ret >= 0 && recvBytes < size );
      return recvBytes;
   }

   public boolean connect() throws Exception {
      try {
         this.socket = new Socket( this.host, this.port );
      } catch( UnknownHostException e ) {
         System.err.println( "Fatal error: Unknown host is specified." );
         System.exit( 1 );
      } catch( IOException e ) {
         System.err.println( "Fatal error: Fail to connect." );
         System.exit( 1 );
      }

      this.in = this.socket.getInputStream();
      this.out = this.socket.getOutputStream();

      // Prepare register message
      int bytes = this.protocol.generateHeader(
         Protocol.PROTO_MAGIC_REQUEST,
         Protocol.PROTO_MAGIC_TO_MASTER,
         Protocol.PROTO_OPCODE_REGISTER,
         0
      );
      this.out.write( this.protocol.buf, 0, bytes );

      bytes = this.read( Protocol.PROTO_HEADER_SIZE );
      if ( bytes == Protocol.PROTO_HEADER_SIZE ) {
         this.protocol.parseHeader( bytes );
         System.out.println( "success" );
      }
      return true;
   }

   public boolean disconnect() {
      return true;
   }

   public boolean get( String key ) {
      return true;
   }

   public boolean set( String key, String value ) {
      return true;
   }

   public boolean update( String key, String value, int offset ) {
      return true;
   }

   public boolean delete( String key ) {
      return true;
   }
}
