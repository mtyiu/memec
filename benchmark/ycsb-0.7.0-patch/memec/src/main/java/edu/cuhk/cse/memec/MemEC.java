package edu.cuhk.cse.memec;

import java.net.Socket;
import java.net.UnknownHostException;

import java.io.BufferedInputStream;
import java.io.OutputStream;
import java.io.IOException;

/**
 * MemEC Java interface.
 * 
 * MemEC Java interface for applications
 */
public class MemEC {
  private Protocol protocol;
  private String host;
  private int port;
  private int instanceId;
  private int id;
  private int fromId;
  private int toId;
  private Socket socket;
  private BufferedInputStream in;
  private OutputStream out;
  private static final boolean IS_DEBUG_MODE = true;

  public static int defaultPort = 9110;
  public static int defaultKeySize = 255;
  public static int defaultChunkSize = 4096;

  public MemEC(int keySize, int chunkSize, String host, int port, int fromId, int toId) {
    this.protocol = new Protocol(keySize, chunkSize);
    this.host = host;
    this.port = port;
    this.instanceId = 0;
    this.id = fromId;
    this.fromId = fromId;
    this.toId = toId;
  }

  public int getDefaultPort() {
    return defaultPort;
  }

  public int getDefaultKeySize() {
    return defaultKeySize;
  }
  
  public int getDefaultChunkSize() {
    return defaultChunkSize;
  }

  public int nextVal() {
    if (this.id == this.toId - 1) {
      this.id = this.fromId;
    } else {
      this.id++;
    }
    return this.id;
  }

  private int read(int size) throws IOException {
    int recvBytes = 0, ret;
    do {
      ret = this.in.read(this.protocol.buf, recvBytes, size - recvBytes);
      if (ret > 0) {
        recvBytes += ret;
      }
    } while (ret >= 0 && recvBytes < size);
    return recvBytes;
  }

  private void debug(String s) {
    if (IS_DEBUG_MODE) {
      System.out.println("\n" + s);
    }
  }

  public boolean connect() {
    try {
      this.socket = new Socket(this.host, this.port);
    } catch(UnknownHostException e) {
      System.err.println("MemEC.connect(): [Error] Unknown host is specified.");
      System.exit(1);
    } catch(IOException e) {
      System.err.println("MemEC.connect(): [Error] Fail to connect.");
      System.exit(1);
    }

    try {
      this.in = new BufferedInputStream(this.socket.getInputStream());
    } catch(IOException e) {
      System.err.println("MemEC.connect(): [Error] Unable to get socket's input stream.");
      System.exit(1);
    }
    try {
      this.out = this.socket.getOutputStream();
    } catch(IOException e) {
      System.err.println("MemEC.connect(): [Error] Unable to get socket's output stream.");
      System.exit(1);
    }

    // Prepare register message
    int qid = this.nextVal();
    int bytes = this.protocol.generateHeader(
        Protocol.PROTO_MAGIC_REQUEST,
        Protocol.PROTO_MAGIC_TO_CLIENT,
        Protocol.PROTO_OPCODE_REGISTER,
        0,
        0,
        qid
    );
    try {
      this.out.write(this.protocol.buf, 0, bytes);
    } catch(IOException e) {
      System.err.println("MemEC.connect(): [Error] Unable to send register request to client.");
      System.exit(1);
    }

    try {
      bytes = this.read(Protocol.PROTO_HEADER_SIZE);
    } catch(IOException e) {
      System.err.println("MemEC.connect(): [Error] Unable to read response from client.");
      return false;
    }
    if (bytes == Protocol.PROTO_HEADER_SIZE) {
      this.protocol.parseHeader(bytes);
      this.instanceId = this.protocol.header.instanceId;
      if (this.protocol.header.id != qid) {
        System.err.println("MemEC.connect(): [Error] The response does not match the request ID.");
        return false;
      }
      return true;
      // this.debug(this.protocol.header.toString());
    } else {
      System.err.println(
          "MemEC.connect(): [Error] Header length mismatch: " + bytes + 
          " vs. " + this.protocol.header.length + "."
      );
      return false;
    }
  }

  public boolean disconnect() {
    try {
      this.socket.close();
    } catch(IOException e) {
      return false;
    }
    return true;
  }

  public String get(String key) {
    byte[] k = key.getBytes();
    return this.get(k, k.length);
  }

  public boolean set(String key, String value) {
    byte[] k = key.getBytes();
    byte[] v = value.getBytes();
    return this.set(k, k.length, v, v.length);
  }

  public boolean update(String key, String value, int offset) {
    byte[] k = key.getBytes();
    byte[] v = value.getBytes();
    return this.update(k, k.length, v, offset, v.length);
  }

  public boolean delete(String key) {
    byte[] k = key.getBytes();
    return this.delete(k, k.length);
  }

  public String get(byte[] key, int keySize) {
    int qid = this.nextVal();
    int bytes = this.protocol.generateKeyHeader(
        Protocol.PROTO_MAGIC_REQUEST,
        Protocol.PROTO_MAGIC_TO_CLIENT,
        Protocol.PROTO_OPCODE_GET,
        this.instanceId, qid,
        keySize, key
    );
    try {
      this.out.write(this.protocol.buf, 0, bytes);
    } catch(IOException e) {
      System.err.println("MemEC.get(): [Error] Unable to send GET request to client.");
      return null;
    }

    try {
      bytes = this.read(Protocol.PROTO_HEADER_SIZE);
    } catch(IOException e) {
      System.err.println("MemEC.get(): [Error] Unable to read GET response from client.");
      return null;
    }
    if (bytes == Protocol.PROTO_HEADER_SIZE) {
      this.protocol.parseHeader(bytes);
      // this.debug(this.protocol.header.toString());
    }

    try {
      bytes = this.read(this.protocol.header.length);
    } catch(IOException e) {
      System.err.println("MemEC.get(): [Error] Unable to read GET response from client.");
      return null;
    }
    if (bytes == this.protocol.header.length) {
      if (this.protocol.header.id != qid) {
        System.err.println("MemEC.get(): [Error] The response does not match the request ID.");
      } else if (this.protocol.header.isSuccessful()) {
        this.protocol.parseKeyValueHeader(bytes, 0);
        // this.debug(this.protocol.keyValueHeader.toString());
        if (this.protocol.keyValueHeader.match(key, keySize)) {
          return this.protocol.keyValueHeader.value();
        }
        System.err.println("MemEC.get(): [Error] The response does not match with the key.");
      } else {
        this.protocol.parseKeyHeader(bytes, 0);
        // this.debug(this.protocol.keyHeader.toString());
        // System.err.println("MemEC.get(): [Error] Key not found.");
      }
    } else {
      System.err.println(
          "MemEC.get(): [Error] Header length mismatch: " + bytes +
          " vs. " + this.protocol.header.length + "."
      );
    }
    return null;
  }

  public boolean set(byte[] key, int keySize, byte[] value, int valueSize) {
    int qid = this.nextVal();
    int bytes = this.protocol.generateKeyValueHeader(
        Protocol.PROTO_MAGIC_REQUEST,
        Protocol.PROTO_MAGIC_TO_CLIENT,
        Protocol.PROTO_OPCODE_SET,
        this.instanceId, qid,
        keySize, key,
        valueSize, value
    );
    try {
      this.out.write(this.protocol.buf, 0, bytes);
    } catch(IOException e) {
      System.err.println("MemEC.set(): [Error] Unable to send SET request to client.");
      return false;
    }

    try {
      bytes = this.read(Protocol.PROTO_HEADER_SIZE);
    } catch(IOException e) {
      System.err.println("MemEC.set(): [Error] Unable to read SET response from client.");
      return false;
    }
    if (bytes == Protocol.PROTO_HEADER_SIZE) {
      this.protocol.parseHeader(bytes);
      // this.debug(this.protocol.header.toString());
    }

    try {
      bytes = this.read(this.protocol.header.length);
    } catch(IOException e) {
      System.err.println("MemEC.set(): [Error] Unable to read SET response from client.");
      return false;
    }
    if (bytes == this.protocol.header.length) {
      if (this.protocol.header.id != qid) {
        System.err.println("MemEC.set(): [Error] The response does not match the request ID.");
      } else if (this.protocol.header.isSuccessful()) {
        this.protocol.parseKeyHeader(bytes, 0);
        // this.debug(this.protocol.keyHeader.toString());
        if (this.protocol.keyHeader.match(key, keySize)) {
          return true;
        } else {
          System.err.println("MemEC.set(): [Error] The response does not match with the key.");
        }
      } else {
        System.err.println("MemEC.set(): [Error] Request failed.");
      }
    } else {
      System.err.println(
          "MemEC.set(): [Error] Header length mismatch: " + bytes +
          " vs. " + this.protocol.header.length + "."
      );
    }
    return false;
  }

  public boolean update(byte[] key, int keySize, byte[] valueUpdate, int valueUpdateOffset, int valueUpdateSize) {
    int qid = this.nextVal();
    int bytes = this.protocol.generateKeyValueUpdateHeader(
        Protocol.PROTO_MAGIC_REQUEST,
        Protocol.PROTO_MAGIC_TO_CLIENT,
        Protocol.PROTO_OPCODE_UPDATE,
        this.instanceId, qid,
        keySize, key,
        valueUpdateOffset, valueUpdateSize, valueUpdate
    );
    try {
      this.out.write(this.protocol.buf, 0, bytes);
    } catch(IOException e) {
      System.err.println("MemEC.update(): [Error] Unable to send UPDATE request to client.");
      return false;
    }

    try {
      bytes = this.read(Protocol.PROTO_HEADER_SIZE);
    } catch(IOException e) {
      System.err.println("MemEC.update(): [Error] Unable to read UPDATE response from client.");
      return false;
    }
    if (bytes == Protocol.PROTO_HEADER_SIZE) {
      this.protocol.parseHeader(bytes);
      // this.debug(this.protocol.header.toString());
    }

    try {
      bytes = this.read(this.protocol.header.length);
    } catch(IOException e) {
      System.err.println("MemEC.update(): [Error] Unable to read UPDATE response from client.");
      return false;
    }
    if (bytes == this.protocol.header.length) {
      if (this.protocol.header.id != qid) {
        System.err.println("MemEC.update(): [Error] The response does not match the request ID.");
      } else if (this.protocol.header.isSuccessful()) {
        this.protocol.parseKeyValueUpdateHeader(bytes, 0);
        // this.debug(this.protocol.keyValueUpdateHeader.toString());
        if (this.protocol.keyValueUpdateHeader.match(key, keySize)) {
          return true;
        } else {
          System.err.println("MemEC.update(): [Error] The response does not match with the key.");
        }
      } else {
        System.err.println("MemEC.update(): [Error] Request failed.");
      }
    } else {
      System.err.println(
          "MemEC.update(): [Error] Header length mismatch: " + bytes +
          " vs. " + this.protocol.header.length + "."
      );
    }
    return false;
  }

  public boolean delete(byte[] key, int keySize) {
    int qid = this.nextVal();
    int bytes = this.protocol.generateKeyHeader(
        Protocol.PROTO_MAGIC_REQUEST,
        Protocol.PROTO_MAGIC_TO_CLIENT,
        Protocol.PROTO_OPCODE_DELETE,
        this.instanceId, qid,
        keySize, key
    );
    try {
      this.out.write(this.protocol.buf, 0, bytes);
    } catch(IOException e) {
      System.err.println("MemEC.delete(): [Error] Unable to send DELETE request to client.");
      return false;
    }

    try {
      bytes = this.read(Protocol.PROTO_HEADER_SIZE);
    } catch(IOException e) {
      System.err.println("MemEC.delete(): [Error] Unable to read DELETE response from client.");
      return false;
    }
    if (bytes == Protocol.PROTO_HEADER_SIZE) {
      this.protocol.parseHeader(bytes);
      // this.debug(this.protocol.header.toString());
    }

    try {
      bytes = this.read(this.protocol.header.length);
    } catch(IOException e) {
      System.err.println("MemEC.delete(): [Error] Unable to read DELETE response from client.");
      return false;
    }
    if (bytes == this.protocol.header.length) {
      if (this.protocol.header.id != qid) {
        System.err.println("MemEC.delete(): [Error] The response does not match the request ID.");
      } else if (this.protocol.header.isSuccessful()) {
        this.protocol.parseKeyHeader(bytes, 0);
        // this.debug(this.protocol.keyHeader.toString());
        if (this.protocol.keyHeader.match(key, keySize)) {
          return true;
        } else {
          System.err.println("MemEC.delete(): [Error] The response does not match with the key.");
        }
      } else {
        System.err.println("MemEC.delete(): [Error] Request failed.");
      }
    } else {
      System.err.println(
          "MemEC.delete(): [Error] Header length mismatch: " + bytes +
          " vs. " + this.protocol.header.length + "."
      );
    }
    return false;
  }
}
