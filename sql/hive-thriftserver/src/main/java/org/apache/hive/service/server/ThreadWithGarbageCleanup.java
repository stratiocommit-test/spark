/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.hive.service.server;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.RawStore;

/**
 * A HiveServer2 thread used to construct new server threads.
 * In particular, this thread ensures an orderly cleanup,
 * when killed by its corresponding ExecutorService.
 */
public class ThreadWithGarbageCleanup extends Thread {
  private static final Log LOG = LogFactory.getLog(ThreadWithGarbageCleanup.class);

  Map<Long, RawStore> threadRawStoreMap =
      ThreadFactoryWithGarbageCleanup.getThreadRawStoreMap();

  public ThreadWithGarbageCleanup(Runnable runnable) {
    super(runnable);
  }

  /**
   * Add any Thread specific garbage cleanup code here.
   * Currently, it shuts down the RawStore object for this thread if it is not null.
   */
  @Override
  public void finalize() throws Throwable {
    cleanRawStore();
    super.finalize();
  }

  private void cleanRawStore() {
    Long threadId = this.getId();
    RawStore threadLocalRawStore = threadRawStoreMap.get(threadId);
    if (threadLocalRawStore != null) {
      LOG.debug("RawStore: " + threadLocalRawStore + ", for the thread: " +
          this.getName()  +  " will be closed now.");
      threadLocalRawStore.shutdown();
      threadRawStoreMap.remove(threadId);
    }
  }

  /**
   * Cache the ThreadLocal RawStore object. Called from the corresponding thread.
   */
  public void cacheThreadLocalRawStore() {
    Long threadId = this.getId();
    RawStore threadLocalRawStore = HiveMetaStore.HMSHandler.getRawStore();
    if (threadLocalRawStore != null && !threadRawStoreMap.containsKey(threadId)) {
      LOG.debug("Adding RawStore: " + threadLocalRawStore + ", for the thread: " +
          this.getName() + " to threadRawStoreMap for future cleanup.");
      threadRawStoreMap.put(threadId, threadLocalRawStore);
    }
  }
}
