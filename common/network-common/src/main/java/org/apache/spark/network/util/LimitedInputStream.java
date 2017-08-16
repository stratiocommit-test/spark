/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.network.util;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.google.common.base.Preconditions;

/**
 * Wraps a {@link InputStream}, limiting the number of bytes which can be read.
 *
 * This code is from Guava's 14.0 source code, because there is no compatible way to
 * use this functionality in both a Guava 11 environment and a Guava &gt;14 environment.
 */
public final class LimitedInputStream extends FilterInputStream {
  private final boolean closeWrappedStream;
  private long left;
  private long mark = -1;

  public LimitedInputStream(InputStream in, long limit) {
    this(in, limit, true);
  }

  /**
   * Create a LimitedInputStream that will read {@code limit} bytes from {@code in}.
   * <p>
   * If {@code closeWrappedStream} is true, this will close {@code in} when it is closed.
   * Otherwise, the stream is left open for reading its remaining content.
   *
   * @param in a {@link InputStream} to read from
   * @param limit the number of bytes to read
   * @param closeWrappedStream whether to close {@code in} when {@link #close} is called
     */
  public LimitedInputStream(InputStream in, long limit, boolean closeWrappedStream) {
    super(in);
    this.closeWrappedStream = closeWrappedStream;
    Preconditions.checkNotNull(in);
    Preconditions.checkArgument(limit >= 0, "limit must be non-negative");
    left = limit;
  }
  @Override public int available() throws IOException {
    return (int) Math.min(in.available(), left);
  }
  // it's okay to mark even if mark isn't supported, as reset won't work
  @Override public synchronized void mark(int readLimit) {
    in.mark(readLimit);
    mark = left;
  }
  @Override public int read() throws IOException {
    if (left == 0) {
      return -1;
    }
    int result = in.read();
    if (result != -1) {
      --left;
    }
    return result;
  }
  @Override public int read(byte[] b, int off, int len) throws IOException {
    if (left == 0) {
      return -1;
    }
    len = (int) Math.min(len, left);
    int result = in.read(b, off, len);
    if (result != -1) {
      left -= result;
    }
    return result;
  }
  @Override public synchronized void reset() throws IOException {
    if (!in.markSupported()) {
      throw new IOException("Mark not supported");
    }
    if (mark == -1) {
      throw new IOException("Mark not set");
    }
    in.reset();
    left = mark;
  }
  @Override public long skip(long n) throws IOException {
    n = Math.min(n, left);
    long skipped = in.skip(n);
    left -= skipped;
    return skipped;
  }

  @Override
  public void close() throws IOException {
    if (closeWrappedStream) {
      super.close();
    }
  }
}
