/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.storage

import org.apache.spark.{SparkConf, SparkFunSuite, Success}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._

/**
 * Test the behavior of StorageStatusListener in response to all relevant events.
 */
class StorageStatusListenerSuite extends SparkFunSuite {
  private val bm1 = BlockManagerId("big", "dog", 1)
  private val bm2 = BlockManagerId("fat", "duck", 2)
  private val taskInfo1 = new TaskInfo(0, 0, 0, 0, "big", "dog", TaskLocality.ANY, false)
  private val taskInfo2 = new TaskInfo(0, 0, 0, 0, "fat", "duck", TaskLocality.ANY, false)
  private val conf = new SparkConf()

  test("block manager added/removed") {
    conf.set("spark.ui.retainedDeadExecutors", "1")
    val listener = new StorageStatusListener(conf)

    // Block manager add
    assert(listener.executorIdToStorageStatus.size === 0)
    listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(1L, bm1, 1000L))
    assert(listener.executorIdToStorageStatus.size === 1)
    assert(listener.executorIdToStorageStatus.get("big").isDefined)
    assert(listener.executorIdToStorageStatus("big").blockManagerId === bm1)
    assert(listener.executorIdToStorageStatus("big").maxMem === 1000L)
    assert(listener.executorIdToStorageStatus("big").numBlocks === 0)
    listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(1L, bm2, 2000L))
    assert(listener.executorIdToStorageStatus.size === 2)
    assert(listener.executorIdToStorageStatus.get("fat").isDefined)
    assert(listener.executorIdToStorageStatus("fat").blockManagerId === bm2)
    assert(listener.executorIdToStorageStatus("fat").maxMem === 2000L)
    assert(listener.executorIdToStorageStatus("fat").numBlocks === 0)

    // Block manager remove
    listener.onBlockManagerRemoved(SparkListenerBlockManagerRemoved(1L, bm1))
    assert(listener.executorIdToStorageStatus.size === 1)
    assert(!listener.executorIdToStorageStatus.get("big").isDefined)
    assert(listener.executorIdToStorageStatus.get("fat").isDefined)
    assert(listener.deadExecutorStorageStatus.size === 1)
    assert(listener.deadExecutorStorageStatus(0).blockManagerId.executorId.equals("big"))
    listener.onBlockManagerRemoved(SparkListenerBlockManagerRemoved(1L, bm2))
    assert(listener.executorIdToStorageStatus.size === 0)
    assert(!listener.executorIdToStorageStatus.get("big").isDefined)
    assert(!listener.executorIdToStorageStatus.get("fat").isDefined)
    assert(listener.deadExecutorStorageStatus.size === 1)
    assert(listener.deadExecutorStorageStatus(0).blockManagerId.executorId.equals("fat"))
  }

  test("task end without updated blocks") {
    val listener = new StorageStatusListener(conf)
    listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(1L, bm1, 1000L))
    listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(1L, bm2, 2000L))
    val taskMetrics = new TaskMetrics

    // Task end with no updated blocks
    assert(listener.executorIdToStorageStatus("big").numBlocks === 0)
    assert(listener.executorIdToStorageStatus("fat").numBlocks === 0)
    listener.onTaskEnd(SparkListenerTaskEnd(1, 0, "obliteration", Success, taskInfo1, taskMetrics))
    assert(listener.executorIdToStorageStatus("big").numBlocks === 0)
    assert(listener.executorIdToStorageStatus("fat").numBlocks === 0)
    listener.onTaskEnd(SparkListenerTaskEnd(1, 0, "obliteration", Success, taskInfo2, taskMetrics))
    assert(listener.executorIdToStorageStatus("big").numBlocks === 0)
    assert(listener.executorIdToStorageStatus("fat").numBlocks === 0)
  }

  test("updated blocks") {
    val listener = new StorageStatusListener(conf)
    listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(1L, bm1, 1000L))
    listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(1L, bm2, 2000L))

    val blockUpdateInfos1 = Seq(
      BlockUpdatedInfo(bm1, RDDBlockId(1, 1), StorageLevel.DISK_ONLY, 0L, 100L),
      BlockUpdatedInfo(bm1, RDDBlockId(1, 2), StorageLevel.DISK_ONLY, 0L, 200L)
    )
    val blockUpdateInfos2 =
      Seq(BlockUpdatedInfo(bm2, RDDBlockId(4, 0), StorageLevel.DISK_ONLY, 0L, 300L))

    // Add some new blocks
    assert(listener.executorIdToStorageStatus("big").numBlocks === 0)
    assert(listener.executorIdToStorageStatus("fat").numBlocks === 0)
    postUpdateBlock(listener, blockUpdateInfos1)
    assert(listener.executorIdToStorageStatus("big").numBlocks === 2)
    assert(listener.executorIdToStorageStatus("fat").numBlocks === 0)
    assert(listener.executorIdToStorageStatus("big").containsBlock(RDDBlockId(1, 1)))
    assert(listener.executorIdToStorageStatus("big").containsBlock(RDDBlockId(1, 2)))
    assert(listener.executorIdToStorageStatus("fat").numBlocks === 0)
    postUpdateBlock(listener, blockUpdateInfos2)
    assert(listener.executorIdToStorageStatus("big").numBlocks === 2)
    assert(listener.executorIdToStorageStatus("fat").numBlocks === 1)
    assert(listener.executorIdToStorageStatus("big").containsBlock(RDDBlockId(1, 1)))
    assert(listener.executorIdToStorageStatus("big").containsBlock(RDDBlockId(1, 2)))
    assert(listener.executorIdToStorageStatus("fat").containsBlock(RDDBlockId(4, 0)))

    // Dropped the blocks
    val droppedBlockInfo1 = Seq(
      BlockUpdatedInfo(bm1, RDDBlockId(1, 1), StorageLevel.NONE, 0L, 0L),
      BlockUpdatedInfo(bm1, RDDBlockId(4, 0), StorageLevel.NONE, 0L, 0L)
    )
    val droppedBlockInfo2 = Seq(
      BlockUpdatedInfo(bm2, RDDBlockId(1, 2), StorageLevel.NONE, 0L, 0L),
      BlockUpdatedInfo(bm2, RDDBlockId(4, 0), StorageLevel.NONE, 0L, 0L)
    )

    postUpdateBlock(listener, droppedBlockInfo1)
    assert(listener.executorIdToStorageStatus("big").numBlocks === 1)
    assert(listener.executorIdToStorageStatus("fat").numBlocks === 1)
    assert(!listener.executorIdToStorageStatus("big").containsBlock(RDDBlockId(1, 1)))
    assert(listener.executorIdToStorageStatus("big").containsBlock(RDDBlockId(1, 2)))
    assert(listener.executorIdToStorageStatus("fat").containsBlock(RDDBlockId(4, 0)))
    postUpdateBlock(listener, droppedBlockInfo2)
    assert(listener.executorIdToStorageStatus("big").numBlocks === 1)
    assert(listener.executorIdToStorageStatus("fat").numBlocks === 0)
    assert(!listener.executorIdToStorageStatus("big").containsBlock(RDDBlockId(1, 1)))
    assert(listener.executorIdToStorageStatus("big").containsBlock(RDDBlockId(1, 2)))
    assert(listener.executorIdToStorageStatus("fat").numBlocks === 0)
  }

  test("unpersist RDD") {
    val listener = new StorageStatusListener(conf)
    listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(1L, bm1, 1000L))
    val blockUpdateInfos1 = Seq(
      BlockUpdatedInfo(bm1, RDDBlockId(1, 1), StorageLevel.DISK_ONLY, 0L, 100L),
      BlockUpdatedInfo(bm1, RDDBlockId(1, 2), StorageLevel.DISK_ONLY, 0L, 200L)
    )
    val blockUpdateInfos2 =
      Seq(BlockUpdatedInfo(bm1, RDDBlockId(4, 0), StorageLevel.DISK_ONLY, 0L, 300L))
    postUpdateBlock(listener, blockUpdateInfos1)
    postUpdateBlock(listener, blockUpdateInfos2)
    assert(listener.executorIdToStorageStatus("big").numBlocks === 3)

    // Unpersist RDD
    listener.onUnpersistRDD(SparkListenerUnpersistRDD(9090))
    assert(listener.executorIdToStorageStatus("big").numBlocks === 3)
    listener.onUnpersistRDD(SparkListenerUnpersistRDD(4))
    assert(listener.executorIdToStorageStatus("big").numBlocks === 2)
    assert(listener.executorIdToStorageStatus("big").containsBlock(RDDBlockId(1, 1)))
    assert(listener.executorIdToStorageStatus("big").containsBlock(RDDBlockId(1, 2)))
    listener.onUnpersistRDD(SparkListenerUnpersistRDD(1))
    assert(listener.executorIdToStorageStatus("big").numBlocks === 0)
  }

  private def postUpdateBlock(
      listener: StorageStatusListener, updateBlockInfos: Seq[BlockUpdatedInfo]): Unit = {
    updateBlockInfos.foreach { updateBlockInfo =>
      listener.onBlockUpdated(SparkListenerBlockUpdated(updateBlockInfo))
    }
  }
}
