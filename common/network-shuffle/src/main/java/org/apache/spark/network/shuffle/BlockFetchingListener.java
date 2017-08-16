/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.network.shuffle;

import java.util.EventListener;

import org.apache.spark.network.buffer.ManagedBuffer;

public interface BlockFetchingListener extends EventListener {
  /**
   * Called once per successfully fetched block. After this call returns, data will be released
   * automatically. If the data will be passed to another thread, the receiver should retain()
   * and release() the buffer on their own, or copy the data to a new buffer.
   */
  void onBlockFetchSuccess(String blockId, ManagedBuffer data);

  /**
   * Called at least once per block upon failures.
   */
  void onBlockFetchFailure(String blockId, Throwable exception);
}
