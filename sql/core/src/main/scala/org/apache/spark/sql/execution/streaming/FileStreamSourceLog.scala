/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.streaming

import java.util.{LinkedHashMap => JLinkedHashMap}
import java.util.Map.Entry

import scala.collection.mutable

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.FileEntry
import org.apache.spark.sql.internal.SQLConf

class FileStreamSourceLog(
    metadataLogVersion: String,
    sparkSession: SparkSession,
    path: String)
  extends CompactibleFileStreamLog[FileEntry](metadataLogVersion, sparkSession, path) {

  import CompactibleFileStreamLog._

  // Configurations about metadata compaction
  protected override val defaultCompactInterval: Int =
    sparkSession.sessionState.conf.fileSourceLogCompactInterval

  require(defaultCompactInterval > 0,
    s"Please set ${SQLConf.FILE_SOURCE_LOG_COMPACT_INTERVAL.key} " +
      s"(was $defaultCompactInterval) to a positive value.")

  protected override val fileCleanupDelayMs =
    sparkSession.sessionState.conf.fileSourceLogCleanupDelay

  protected override val isDeletingExpiredLog = sparkSession.sessionState.conf.fileSourceLogDeletion

  private implicit val formats = Serialization.formats(NoTypeHints)

  // A fixed size log entry cache to cache the file entries belong to the compaction batch. It is
  // used to avoid scanning the compacted log file to retrieve it's own batch data.
  private val cacheSize = compactInterval
  private val fileEntryCache = new JLinkedHashMap[Long, Array[FileEntry]] {
    override def removeEldestEntry(eldest: Entry[Long, Array[FileEntry]]): Boolean = {
      size() > cacheSize
    }
  }

  def compactLogs(logs: Seq[FileEntry]): Seq[FileEntry] = {
    logs
  }

  override def add(batchId: Long, logs: Array[FileEntry]): Boolean = {
    if (super.add(batchId, logs)) {
      if (isCompactionBatch(batchId, compactInterval)) {
        fileEntryCache.put(batchId, logs)
      }
      true
    } else {
      false
    }
  }

  override def get(startId: Option[Long], endId: Option[Long]): Array[(Long, Array[FileEntry])] = {
    val startBatchId = startId.getOrElse(0L)
    val endBatchId = endId.orElse(getLatest().map(_._1)).getOrElse(0L)

    val (existedBatches, removedBatches) = (startBatchId to endBatchId).map { id =>
      if (isCompactionBatch(id, compactInterval) && fileEntryCache.containsKey(id)) {
        (id, Some(fileEntryCache.get(id)))
      } else {
        val logs = super.get(id).map(_.filter(_.batchId == id))
        (id, logs)
      }
    }.partition(_._2.isDefined)

    // The below code may only be happened when original metadata log file has been removed, so we
    // have to get the batch from latest compacted log file. This is quite time-consuming and may
    // not be happened in the current FileStreamSource code path, since we only fetch the
    // latest metadata log file.
    val searchKeys = removedBatches.map(_._1)
    val retrievedBatches = if (searchKeys.nonEmpty) {
      logWarning(s"Get batches from removed files, this is unexpected in the current code path!!!")
      val latestBatchId = getLatest().map(_._1).getOrElse(-1L)
      if (latestBatchId < 0) {
        Map.empty[Long, Option[Array[FileEntry]]]
      } else {
        val latestCompactedBatchId = getAllValidBatches(latestBatchId, compactInterval)(0)
        val allLogs = new mutable.HashMap[Long, mutable.ArrayBuffer[FileEntry]]

        super.get(latestCompactedBatchId).foreach { entries =>
          entries.foreach { e =>
            allLogs.put(e.batchId, allLogs.getOrElse(e.batchId, mutable.ArrayBuffer()) += e)
          }
        }

        searchKeys.map(id => id -> allLogs.get(id).map(_.toArray)).filter(_._2.isDefined).toMap
      }
    } else {
      Map.empty[Long, Option[Array[FileEntry]]]
    }

    (existedBatches ++ retrievedBatches).map(i => i._1 -> i._2.get).toArray.sortBy(_._1)
  }
}

object FileStreamSourceLog {
  val VERSION = "v1"
}
