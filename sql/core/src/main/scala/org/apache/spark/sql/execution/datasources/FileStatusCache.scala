/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.datasources

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import com.google.common.cache._
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{SerializableConfiguration, SizeEstimator}

/**
 * A cache of the leaf files of partition directories. We cache these files in order to speed
 * up iterated queries over the same set of partitions. Otherwise, each query would have to
 * hit remote storage in order to gather file statistics for physical planning.
 *
 * Each resolved catalog table has its own FileStatusCache. When the backing relation for the
 * table is refreshed via refreshTable() or refreshByPath(), this cache will be invalidated.
 */
abstract class FileStatusCache {
  /**
   * @return the leaf files for the specified path from this cache, or None if not cached.
   */
  def getLeafFiles(path: Path): Option[Array[FileStatus]] = None

  /**
   * Saves the given set of leaf files for a path in this cache.
   */
  def putLeafFiles(path: Path, leafFiles: Array[FileStatus]): Unit

  /**
   * Invalidates all data held by this cache.
   */
  def invalidateAll(): Unit
}

object FileStatusCache {
  private var sharedCache: SharedInMemoryCache = null

  /**
   * @return a new FileStatusCache based on session configuration. Cache memory quota is
   *         shared across all clients.
   */
  def newCache(session: SparkSession): FileStatusCache = {
    synchronized {
      if (session.sqlContext.conf.manageFilesourcePartitions &&
          session.sqlContext.conf.filesourcePartitionFileCacheSize > 0) {
        if (sharedCache == null) {
          sharedCache = new SharedInMemoryCache(
            session.sqlContext.conf.filesourcePartitionFileCacheSize)
        }
        sharedCache.getForNewClient()
      } else {
        NoopCache
      }
    }
  }

  def resetForTesting(): Unit = synchronized {
    sharedCache = null
  }
}

/**
 * An implementation that caches partition file statuses in memory.
 *
 * @param maxSizeInBytes max allowable cache size before entries start getting evicted
 */
private class SharedInMemoryCache(maxSizeInBytes: Long) extends Logging {
  import FileStatusCache._

  // Opaque object that uniquely identifies a shared cache user
  private type ClientId = Object

  private val warnedAboutEviction = new AtomicBoolean(false)

  // we use a composite cache key in order to distinguish entries inserted by different clients
  private val cache: Cache[(ClientId, Path), Array[FileStatus]] = CacheBuilder.newBuilder()
    .weigher(new Weigher[(ClientId, Path), Array[FileStatus]] {
      override def weigh(key: (ClientId, Path), value: Array[FileStatus]): Int = {
        (SizeEstimator.estimate(key) + SizeEstimator.estimate(value)).toInt
      }})
    .removalListener(new RemovalListener[(ClientId, Path), Array[FileStatus]]() {
      override def onRemoval(removed: RemovalNotification[(ClientId, Path), Array[FileStatus]]) = {
        if (removed.getCause() == RemovalCause.SIZE &&
            warnedAboutEviction.compareAndSet(false, true)) {
          logWarning(
            "Evicting cached table partition metadata from memory due to size constraints " +
            "(spark.sql.hive.filesourcePartitionFileCacheSize = " + maxSizeInBytes + " bytes). " +
            "This may impact query planning performance.")
        }
      }})
    .maximumWeight(maxSizeInBytes)
    .build()

  /**
   * @return a FileStatusCache that does not share any entries with any other client, but does
   *         share memory resources for the purpose of cache eviction.
   */
  def getForNewClient(): FileStatusCache = new FileStatusCache {
    val clientId = new Object()

    override def getLeafFiles(path: Path): Option[Array[FileStatus]] = {
      Option(cache.getIfPresent((clientId, path)))
    }

    override def putLeafFiles(path: Path, leafFiles: Array[FileStatus]): Unit = {
      cache.put((clientId, path), leafFiles.toArray)
    }

    override def invalidateAll(): Unit = {
      cache.asMap.asScala.foreach { case (key, value) =>
        if (key._1 == clientId) {
          cache.invalidate(key)
        }
      }
    }
  }
}

/**
 * A non-caching implementation used when partition file status caching is disabled.
 */
object NoopCache extends FileStatusCache {
  override def getLeafFiles(path: Path): Option[Array[FileStatus]] = None
  override def putLeafFiles(path: Path, leafFiles: Array[FileStatus]): Unit = {}
  override def invalidateAll(): Unit = {}
}
