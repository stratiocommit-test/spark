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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;

import org.apache.hadoop.hive.metastore.RawStore;

/**
 * A ThreadFactory for constructing new HiveServer2 threads that lets you plug
 * in custom cleanup code to be called before this thread is GC-ed.
 * Currently cleans up the following:
 * 1. ThreadLocal RawStore object:
 * In case of an embedded metastore, HiveServer2 threads (foreground & background)
 * end up caching a ThreadLocal RawStore object. The ThreadLocal RawStore object has
 * an instance of PersistenceManagerFactory & PersistenceManager.
 * The PersistenceManagerFactory keeps a cache of PersistenceManager objects,
 * which are only removed when PersistenceManager#close method is called.
 * HiveServer2 uses ExecutorService for managing thread pools for foreground & background threads.
 * ExecutorService unfortunately does not provide any hooks to be called,
 * when a thread from the pool is terminated.
 * As a solution, we're using this ThreadFactory to keep a cache of RawStore objects per thread.
 * And we are doing clean shutdown in the finalizer for each thread.
 */
public class ThreadFactoryWithGarbageCleanup implements ThreadFactory {

  private static Map<Long, RawStore> threadRawStoreMap = new ConcurrentHashMap<Long, RawStore>();

  private final String namePrefix;

  public ThreadFactoryWithGarbageCleanup(String threadPoolName) {
    namePrefix = threadPoolName;
  }

  @Override
  public Thread newThread(Runnable runnable) {
    Thread newThread = new ThreadWithGarbageCleanup(runnable);
    newThread.setName(namePrefix + ": Thread-" + newThread.getId());
    return newThread;
  }

  public static Map<Long, RawStore> getThreadRawStoreMap() {
    return threadRawStoreMap;
  }
}
