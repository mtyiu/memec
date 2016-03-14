/**
 * MemEC.
 *
 * MemEC Java interface
 */
package edu.cuhk.cse.memec;

/**
 * MemEC protocol.
 *
 * MemEC protocol for Java interface
 */
public class Protocol {
  public byte[] buf;
  private byte from;

  private static final int PROTO_BUF_MIN_SIZE = 65536;

  /*************************
   *  Magic byte (1 byte). *
   *************************/
  // (Bit: 0-2) //
  public static final byte PROTO_MAGIC_REQUEST          = 0x01; // -----001
  public static final byte PROTO_MAGIC_RESPONSE_SUCCESS = 0x02; // -----010
  public static final byte PROTO_MAGIC_RESPONSE_FAILURE = 0x03; // -----011
  // (Bit: 3-4) //
  public static final byte PROTO_MAGIC_FROM_APPLICATION = 0x00; // ---00---
  public static final byte PROTO_MAGIC_FROM_CLIENT      = 0x10; // ---10---
   // (Bit: 5-6) //
  public static final byte PROTO_MAGIC_TO_APPLICATION    = 0x00; // -00-----
  public static final byte PROTO_MAGIC_TO_CLIENT        = 0x40; // -10-----
  // (Bit: 7): Reserved //

  /********************
   * Opcode (1 byte). *
   ********************/
  public static final byte PROTO_OPCODE_REGISTER = 0x00;
  public static final byte PROTO_OPCODE_GET      = 0x01;
  public static final byte PROTO_OPCODE_SET      = 0x02;
  public static final byte PROTO_OPCODE_UPDATE   = 0x03;
  public static final byte PROTO_OPCODE_DELETE   = 0x04;

  /**********************
   * Key size (1 byte). *
   **********************/
  public static final int MAXIMUM_KEY_SIZE = 255;
  /************************
   * Value size (3 byte). *
   ************************/
  public static final int MAXIMUM_VALUE_SIZE = 16777215;
  /*************************
   * Protocol header size. *
   *************************/
  public static final byte PROTO_HEADER_SIZE            = 16;
  public static final byte PROTO_KEY_SIZE               = 1;
  public static final byte PROTO_KEY_VALUE_SIZE         = 4;
  public static final byte PROTO_KEY_VALUE_UPDATE_SIZE  = 7;

  /**
   * MemEC protocol header (general).
   */
  public static class Header {
    public byte magic, from, to, opcode;
    public int length, instanceId, id, timestamp;

    public boolean isSuccessful() {
      return this.magic == PROTO_MAGIC_RESPONSE_SUCCESS;
    }

    public String toString() {
      String ret = "(";
      switch(magic) {
      case PROTO_MAGIC_REQUEST:
        ret += "PROTO_MAGIC_REQUEST, ";
        break;
      case PROTO_MAGIC_RESPONSE_SUCCESS:
        ret += "PROTO_MAGIC_RESPONSE_SUCCESS, ";
        break;
      case PROTO_MAGIC_RESPONSE_FAILURE:
        ret += "PROTO_MAGIC_RESPONSE_FAILURE, ";
        break;
      default:
        return "";
      }

      switch(from) {
      case PROTO_MAGIC_FROM_APPLICATION:
        ret += "PROTO_MAGIC_FROM_APPLICATION, ";
        break;
      case PROTO_MAGIC_FROM_CLIENT:
        ret += "PROTO_MAGIC_FROM_CLIENT, ";
        break;
      default:
        return ret;
      }

      switch(to) {
      case PROTO_MAGIC_TO_APPLICATION:
        ret += "PROTO_MAGIC_TO_APPLICATION, ";
        break;
      case PROTO_MAGIC_TO_CLIENT:
        ret += "PROTO_MAGIC_TO_CLIENT, ";
        break;
      default:
        return ret;
      }

      switch(opcode) {
      case PROTO_OPCODE_REGISTER:
        ret += "PROTO_OPCODE_REGISTER";
        break;
      case PROTO_OPCODE_GET:
        ret += "PROTO_OPCODE_GET";
        break;
      case PROTO_OPCODE_SET:
        ret += "PROTO_OPCODE_SET";
        break;
      case PROTO_OPCODE_UPDATE:
        ret += "PROTO_OPCODE_UPDATE";
        break;
      case PROTO_OPCODE_DELETE:
        ret += "PROTO_OPCODE_DELETE";
        break;
      default:
        return ret;
      }

      ret += ", " + length + ")";

      return ret;
    }
  }

  /**
   * MemEC protocol header (key).
   */
  public static class KeyHeader {
    public int keySize;
    public int keyPos;
    byte[] data;

    public String key() {
      return new String(data, keyPos, keySize);
    }

    public String toString() {
      return "Key: " + key() + " (key size: " + keySize + ")";
    }

    public boolean match(byte[] k, int len) {
      if (len == this.keySize) {
        for (int i = 0; i < len; i++) {
          if (k[i] != this.data[keyPos + i]) {
            return false;
          }
        }
        return true;
      }
      return false;
    }
  }

  /**
   * MemEC protocol header (value).
   */
  public static class KeyValueHeader {
    public int keySize, valueSize;
    public int keyPos, valuePos;
    byte[] data;

    public String key() {
      return new String(data, keyPos, keySize);
    }

    public String value() {
      return new String(data, valuePos, valueSize);
    }

    public String toString() {
      return "Key: " + key() + " (key size: " + keySize + "); Value: " + value() + " (value size: " + valueSize + ")";
    }

    public boolean match(byte[] k, int len) {
      if (len == this.keySize) {
        for (int i = 0; i < len; i++) {
          if (k[i] != this.data[keyPos + i]) {
            return false;
          }
        }
        return true;
      }
      return false;
    }
  }

  /**
   * MemEC protocol header (key-value update).
   */
  public static class KeyValueUpdateHeader {
    public int keySize, valueUpdateSize, valueUpdateOffset;
    public int keyPos;
    byte[] data;

    public String key() {
      return new String(data, keyPos, keySize);
    }

    public String toString() {
      return "Key: " + key() + " (key size: " + keySize + "); Value update: (value update size: " +
          valueUpdateSize + "; offset: " + valueUpdateOffset + ")";
    }

    public boolean match(byte[] k, int len) {
      if (len == this.keySize) {
        for (int i = 0; i < len; i++) {
          if (k[i] != this.data[keyPos + i]) {
            return false;
          }
        }
        return true;
      }
      return false;
    }
  }

  public Header header = new Header();
  public KeyHeader keyHeader = new KeyHeader();
  public KeyValueHeader keyValueHeader = new KeyValueHeader();
  public KeyValueUpdateHeader keyValueUpdateHeader = new KeyValueUpdateHeader();

  public static int getSuggestedBufferSize(int keySize, int chunkSize) {
    int ret = PROTO_HEADER_SIZE + PROTO_KEY_VALUE_SIZE + keySize + chunkSize;
    if ((ret & 4095) > 0) {
      ret >>= 12;
      ret +=1;
      ret <<= 12;
    }

    ret <<= 1;
    if (ret < PROTO_BUF_MIN_SIZE) {
      ret = PROTO_BUF_MIN_SIZE;
    }

    return ret;
  }

  public Protocol(int keySize, int chunkSize) {
    this.buf = new byte[Protocol.getSuggestedBufferSize(keySize, chunkSize)];
    this.from = PROTO_MAGIC_FROM_APPLICATION;
    this.keyHeader.data = this.buf;
    this.keyValueHeader.data = this.buf;
    this.keyValueUpdateHeader.data = this.buf;
  }

  public int generateHeader(byte magic, byte to, byte opcode, int length, int instanceId, int id) {
    return this.generateHeader(magic, to, opcode, length, instanceId, id, 0);
  }

  public int generateHeader(byte magic, byte to, byte opcode, int length, int instanceId, int id, int timestamp) {
    this.buf[0]  = (byte)(magic & 0x07);
    this.buf[0] |= (byte)(this.from & 0x18);
    this.buf[0] |= (byte)(to & 0x60);

    this.buf[ 1] = (byte)(opcode & 0xFF);

    this.buf[ 2] = (byte)((length >> 24) & 0xFF);
    this.buf[ 3] = (byte)((length >> 16) & 0xFF);
    this.buf[ 4] = (byte)((length >>  8) & 0xFF);
    this.buf[ 5] = (byte)((length) & 0xFF);

    this.buf[ 6] = (byte)((instanceId >>  8) & 0xFF);
    this.buf[ 7] = (byte)((instanceId) & 0xFF);

    this.buf[ 8] = (byte)((id >> 24) & 0xFF);
    this.buf[ 9] = (byte)((id >> 16) & 0xFF);
    this.buf[10] = (byte)((id >>  8) & 0xFF);
    this.buf[11] = (byte)((id) & 0xFF);

    this.buf[12] = (byte)((timestamp >> 24) & 0xFF);
    this.buf[13] = (byte)((timestamp >> 16) & 0xFF);
    this.buf[14] = (byte)((timestamp >>  8) & 0xFF);
    this.buf[15] = (byte)((timestamp) & 0xFF);
    return PROTO_HEADER_SIZE;
  }

  public int generateKeyHeader(byte magic, byte to, byte opcode, int instanceId, int id, int keySize, byte[] key) {
    int ret = this.generateHeader(magic, to, opcode, PROTO_KEY_SIZE + keySize, instanceId, id);

    this.buf[ret] = (byte)(keySize & 0xFF);
    ret++;

    System.arraycopy(key, 0, this.buf, ret, keySize);
    ret += keySize;

    return ret;
  }

  public int generateKeyValueHeader(byte magic, byte to, byte opcode, int instanceId, int id, int keySize, 
      byte[] key, int valueSize, byte[] value
  ) {
    int ret = this.generateHeader(magic, to, opcode, PROTO_KEY_VALUE_SIZE + keySize + valueSize, instanceId, id);

    this.buf[ret   ] = (byte)(keySize & 0xFF);
    this.buf[ret + 1] = (byte)((valueSize >> 16) & 0xFF);
    this.buf[ret + 2] = (byte)((valueSize >>  8) & 0xFF);
    this.buf[ret + 3] = (byte)((valueSize) & 0xFF);

    ret += PROTO_KEY_VALUE_SIZE;

    System.arraycopy(key, 0, this.buf, ret, keySize);
    ret += keySize;

    if (valueSize > 0) {
      System.arraycopy(value, 0, this.buf, ret, valueSize);
      ret += valueSize;
    }

    return ret;
  }

  public int generateKeyValueUpdateHeader(byte magic, byte to, byte opcode, int instanceId, int id, int keySize,
      byte[] key, int valueUpdateOffset, int valueUpdateSize, byte[] valueUpdate
  ) {
    int ret = this.generateHeader(magic, to, opcode, PROTO_KEY_VALUE_UPDATE_SIZE + keySize + 
        (valueUpdate != null ? valueUpdateSize : 0), instanceId, id);

    this.buf[ret   ] = (byte)(keySize & 0xFF);
    this.buf[ret + 1] = (byte)((valueUpdateSize  >> 16) & 0xFF);
    this.buf[ret + 2] = (byte)((valueUpdateSize  >>  8) & 0xFF);
    this.buf[ret + 3] = (byte)((valueUpdateSize) & 0xFF);
    this.buf[ret + 4] = (byte)((valueUpdateOffset >> 16) & 0xFF);
    this.buf[ret + 5] = (byte)((valueUpdateOffset >>  8) & 0xFF);
    this.buf[ret + 6] = (byte)((valueUpdateOffset) & 0xFF);
    ret += PROTO_KEY_VALUE_UPDATE_SIZE;

    System.arraycopy(key, 0, this.buf, ret, keySize);
    ret += keySize;

    if (valueUpdateSize > 0 && valueUpdate != null) {
      System.arraycopy(valueUpdate, 0, this.buf, ret, valueUpdateSize);
      ret += valueUpdateSize;
    }

    return ret;
  }

  public boolean parseHeader(int size) {
    long tmp;

    if (size < PROTO_HEADER_SIZE) {
      return false;
    }

    header.magic  = (byte)(this.buf[0] & 0x07);
    header.from  = (byte)(this.buf[0] & 0x18);
    header.to    = (byte)(this.buf[0] & 0x60);
    header.opcode = (byte)(this.buf[1] & 0xFF);

    tmp        = ((((long) this.buf[2]) & 0xFF) << 24) |
                    ((((long) this.buf[3]) & 0xFF) << 16) |
                    ((((long) this.buf[4]) & 0xFF) <<  8) |
                    ((((long) this.buf[5]) & 0xFF));
    header.length = (int) tmp;

    tmp        = ((((long) this.buf[6]) & 0xFF) <<  8) |
                    ((((long) this.buf[7]) & 0xFF));
    header.instanceId = (int) tmp;

    tmp        = ((((long) this.buf[ 8]) & 0xFF) << 24) |
                    ((((long) this.buf[ 9]) & 0xFF) << 16) |
                    ((((long) this.buf[10]) & 0xFF) <<  8) |
                    ((((long) this.buf[11]) & 0xFF));
    header.id     = (int) tmp;

    tmp        = ((((long) this.buf[12]) & 0xFF) << 24) |
                    ((((long) this.buf[13]) & 0xFF) << 16) |
                    ((((long) this.buf[14]) & 0xFF) <<  8) |
                    ((((long) this.buf[15]) & 0xFF));
    header.timestamp = (int) tmp;

    switch(header.magic) {
    case PROTO_MAGIC_REQUEST:
    case PROTO_MAGIC_RESPONSE_SUCCESS:
    case PROTO_MAGIC_RESPONSE_FAILURE:
      break;
    default:
      return false;
    }

    switch(header.from) {
    case PROTO_MAGIC_FROM_APPLICATION:
    case PROTO_MAGIC_FROM_CLIENT:
      break;
    default:
      return false;
    }

    switch(header.to) {
    case PROTO_MAGIC_TO_APPLICATION:
    case PROTO_MAGIC_TO_CLIENT:
      break;
    default:
      return false;
    }

    switch(header.opcode) {
    case PROTO_OPCODE_REGISTER:
    case PROTO_OPCODE_GET:
    case PROTO_OPCODE_SET:
    case PROTO_OPCODE_UPDATE:
    case PROTO_OPCODE_DELETE:
      break;
    default:
      return false;
    }

    return true;
  }

  public boolean parseKeyHeader(int size, int offset) {
    if (size < PROTO_KEY_SIZE) {
      return false;
    }

    keyHeader.keySize = this.buf[offset];

    if (size < PROTO_KEY_SIZE + keyHeader.keySize) {
      return false;
    }

    keyHeader.keyPos = offset + PROTO_KEY_SIZE;

    return true;
  }

  public boolean parseKeyValueHeader(int size, int offset) {
    if (size < PROTO_KEY_VALUE_SIZE) {
      return false;
    }

    long tmp;

    keyValueHeader.keySize = this.buf[offset];

    tmp = ((((long) this.buf[offset + 1]) & 0xFF) << 16) |
        ((((long) this.buf[offset + 2]) & 0xFF) <<  8) |
        ((((long) this.buf[offset + 3]) & 0xFF));
    keyValueHeader.valueSize = (int) tmp;

    if (size < PROTO_KEY_VALUE_SIZE + keyValueHeader.keySize + keyValueHeader.valueSize) {
      return false;
    }

    keyValueHeader.keyPos = offset + PROTO_KEY_VALUE_SIZE;
    keyValueHeader.valuePos = keyValueHeader.keyPos + keyValueHeader.keySize;

    return true;
  }

  public boolean parseKeyValueUpdateHeader(int size, int offset) {
    if (size < PROTO_KEY_VALUE_UPDATE_SIZE) {
      return false;
    }

    long tmp;

    keyValueUpdateHeader.keySize = this.buf[offset];

    tmp = ((((long) this.buf[offset + 1]) & 0xFF) << 16) |
        ((((long) this.buf[offset + 2]) & 0xFF) <<  8) |
        ((((long) this.buf[offset + 3]) & 0xFF));
    keyValueUpdateHeader.valueUpdateSize = (int) tmp;

    tmp = ((((long) this.buf[offset + 4]) & 0xFF) << 16) |
        ((((long) this.buf[offset + 5]) & 0xFF) <<  8) |
        ((((long) this.buf[offset + 6]) & 0xFF));
    keyValueUpdateHeader.valueUpdateOffset = (int) tmp;

    if (size < PROTO_KEY_VALUE_UPDATE_SIZE + keyValueUpdateHeader.keySize) {
      return false;
    }

    keyValueUpdateHeader.keyPos = offset + PROTO_KEY_VALUE_UPDATE_SIZE;

    return true;
  }
}
