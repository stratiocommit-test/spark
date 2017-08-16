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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.streaming.Time

/**
 * :: DeveloperApi ::
 * Class having information on output operations.
 * @param batchTime Time of the batch
 * @param id Id of this output operation. Different output operations have different ids in a batch.
 * @param name The name of this output operation.
 * @param description The description of this output operation.
 * @param startTime Clock time of when the output operation started processing
 * @param endTime Clock time of when the output operation started processing
 * @param failureReason Failure reason if this output operation fails
 */
@DeveloperApi
case class OutputOperationInfo(
    batchTime: Time,
    id: Int,
    name: String,
    description: String,
    startTime: Option[Long],
    endTime: Option[Long],
    failureReason: Option[String]) {

  /**
   * Return the duration of this output operation.
   */
  def duration: Option[Long] = for (s <- startTime; e <- endTime) yield e - s
}
