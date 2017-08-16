/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.network.client;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Callback for streaming data. Stream data will be offered to the
 * {@link #onData(String, ByteBuffer)} method as it arrives. Once all the stream data is received,
 * {@link #onComplete(String)} will be called.
 * <p>
 * The network library guarantees that a single thread will call these methods at a time, but
 * different call may be made by different threads.
 */
public interface StreamCallback {
  /** Called upon receipt of stream data. */
  void onData(String streamId, ByteBuffer buf) throws IOException;

  /** Called when all data from the stream has been received. */
  void onComplete(String streamId) throws IOException;

  /** Called if there's an error reading data from the stream. */
  void onFailure(String streamId, Throwable cause) throws IOException;
}
