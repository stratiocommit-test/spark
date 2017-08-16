/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.memory

import org.apache.spark.SparkConf
import org.apache.spark.storage.BlockId

class TestMemoryManager(conf: SparkConf)
  extends MemoryManager(conf, numCores = 1, Long.MaxValue, Long.MaxValue) {

  override private[memory] def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = {
    if (oomOnce) {
      oomOnce = false
      0
    } else if (available >= numBytes) {
      available -= numBytes
      numBytes
    } else {
      val grant = available
      available = 0
      grant
    }
  }
  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = true
  override def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
     memoryMode: MemoryMode): Boolean = true
  override def releaseStorageMemory(numBytes: Long, memoryMode: MemoryMode): Unit = {}
  override private[memory] def releaseExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Unit = {
    available += numBytes
  }
  override def maxOnHeapStorageMemory: Long = Long.MaxValue

  override def maxOffHeapStorageMemory: Long = 0L

  private var oomOnce = false
  private var available = Long.MaxValue

  def markExecutionAsOutOfMemoryOnce(): Unit = {
    oomOnce = true
  }

  def limit(avail: Long): Unit = {
    available = avail
  }

}
