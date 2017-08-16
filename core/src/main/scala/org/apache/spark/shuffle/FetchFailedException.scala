/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.shuffle

import org.apache.spark.{FetchFailed, TaskFailedReason}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

/**
 * Failed to fetch a shuffle block. The executor catches this exception and propagates it
 * back to DAGScheduler (through TaskEndReason) so we'd resubmit the previous stage.
 *
 * Note that bmAddress can be null.
 */
private[spark] class FetchFailedException(
    bmAddress: BlockManagerId,
    shuffleId: Int,
    mapId: Int,
    reduceId: Int,
    message: String,
    cause: Throwable = null)
  extends Exception(message, cause) {

  def this(
      bmAddress: BlockManagerId,
      shuffleId: Int,
      mapId: Int,
      reduceId: Int,
      cause: Throwable) {
    this(bmAddress, shuffleId, mapId, reduceId, cause.getMessage, cause)
  }

  def toTaskFailedReason: TaskFailedReason = FetchFailed(bmAddress, shuffleId, mapId, reduceId,
    Utils.exceptionString(this))
}

/**
 * Failed to get shuffle metadata from [[org.apache.spark.MapOutputTracker]].
 */
private[spark] class MetadataFetchFailedException(
    shuffleId: Int,
    reduceId: Int,
    message: String)
  extends FetchFailedException(null, shuffleId, -1, reduceId, message)
