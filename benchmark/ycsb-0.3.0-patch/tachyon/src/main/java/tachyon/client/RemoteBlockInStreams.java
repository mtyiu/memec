package tachyon.client;

import java.io.IOException;
import tachyon.conf.TachyonConf;

public final class RemoteBlockInStreams {
  private RemoteBlockInStreams() {}

  public static RemoteBlockInStream create(final TachyonFile file, final ReadType readType, final int blockIndex, TachyonConf conf) throws IOException {
    return new RemoteBlockInStream(file, readType, blockIndex, conf);
  }
}
