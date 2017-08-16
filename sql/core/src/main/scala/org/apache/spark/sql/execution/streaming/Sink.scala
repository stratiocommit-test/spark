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

import org.apache.spark.sql.DataFrame

/**
 * An interface for systems that can collect the results of a streaming query. In order to preserve
 * exactly once semantics a sink must be idempotent in the face of multiple attempts to add the same
 * batch.
 */
trait Sink {

  /**
   * Adds a batch of data to this sink. The data for a given `batchId` is deterministic and if
   * this method is called more than once with the same batchId (which will happen in the case of
   * failures), then `data` should only be added once.
   *
   * Note: You cannot apply any operators on `data` except consuming it (e.g., `collect/foreach`).
   * Otherwise, you may get a wrong result.
   */
  def addBatch(batchId: Long, data: DataFrame): Unit
}
