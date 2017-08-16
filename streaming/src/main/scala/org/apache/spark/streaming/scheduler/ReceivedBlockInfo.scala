/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.scheduler

import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.receiver.{ReceivedBlockStoreResult, WriteAheadLogBasedStoreResult}
import org.apache.spark.streaming.util.WriteAheadLogRecordHandle

/** Information about blocks received by the receiver */
private[streaming] case class ReceivedBlockInfo(
    streamId: Int,
    numRecords: Option[Long],
    metadataOption: Option[Any],
    blockStoreResult: ReceivedBlockStoreResult
  ) {

  require(numRecords.isEmpty || numRecords.get >= 0, "numRecords must not be negative")

  @volatile private var _isBlockIdValid = true

  def blockId: StreamBlockId = blockStoreResult.blockId

  def walRecordHandleOption: Option[WriteAheadLogRecordHandle] = {
    blockStoreResult match {
      case walStoreResult: WriteAheadLogBasedStoreResult => Some(walStoreResult.walRecordHandle)
      case _ => None
    }
  }

  /** Is the block ID valid, that is, is the block present in the Spark executors. */
  def isBlockIdValid(): Boolean = _isBlockIdValid

  /**
   * Set the block ID as invalid. This is useful when it is known that the block is not present
   * in the Spark executors.
   */
  def setBlockIdInvalid(): Unit = {
    _isBlockIdValid = false
  }
}

