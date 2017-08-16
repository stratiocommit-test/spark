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

import scala.collection.mutable

import org.apache.spark.scheduler._

private[spark] case class BlockUIData(
    blockId: BlockId,
    location: String,
    storageLevel: StorageLevel,
    memSize: Long,
    diskSize: Long)

/**
 * The aggregated status of stream blocks in an executor
 */
private[spark] case class ExecutorStreamBlockStatus(
    executorId: String,
    location: String,
    blocks: Seq[BlockUIData]) {

  def totalMemSize: Long = blocks.map(_.memSize).sum

  def totalDiskSize: Long = blocks.map(_.diskSize).sum

  def numStreamBlocks: Int = blocks.size

}

private[spark] class BlockStatusListener extends SparkListener {

  private val blockManagers =
    new mutable.HashMap[BlockManagerId, mutable.HashMap[BlockId, BlockUIData]]

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {
    val blockId = blockUpdated.blockUpdatedInfo.blockId
    if (!blockId.isInstanceOf[StreamBlockId]) {
      // Now we only monitor StreamBlocks
      return
    }
    val blockManagerId = blockUpdated.blockUpdatedInfo.blockManagerId
    val storageLevel = blockUpdated.blockUpdatedInfo.storageLevel
    val memSize = blockUpdated.blockUpdatedInfo.memSize
    val diskSize = blockUpdated.blockUpdatedInfo.diskSize

    synchronized {
      // Drop the update info if the block manager is not registered
      blockManagers.get(blockManagerId).foreach { blocksInBlockManager =>
        if (storageLevel.isValid) {
          blocksInBlockManager.put(blockId,
            BlockUIData(
              blockId,
              blockManagerId.hostPort,
              storageLevel,
              memSize,
              diskSize)
          )
        } else {
          // If isValid is not true, it means we should drop the block.
          blocksInBlockManager -= blockId
        }
      }
    }
  }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    synchronized {
      blockManagers.put(blockManagerAdded.blockManagerId, mutable.HashMap())
    }
  }

  override def onBlockManagerRemoved(
      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = synchronized {
    blockManagers -= blockManagerRemoved.blockManagerId
  }

  def allExecutorStreamBlockStatus: Seq[ExecutorStreamBlockStatus] = synchronized {
    blockManagers.map { case (blockManagerId, blocks) =>
      ExecutorStreamBlockStatus(
        blockManagerId.executorId, blockManagerId.hostPort, blocks.values.toSeq)
    }.toSeq
  }
}
