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
 * Class having information on completed batches.
 * @param batchTime   Time of the batch
 * @param streamIdToInputInfo A map of input stream id to its input info
 * @param submissionTime  Clock time of when jobs of this batch was submitted to
 *                        the streaming scheduler queue
 * @param processingStartTime Clock time of when the first job of this batch started processing
 * @param processingEndTime Clock time of when the last job of this batch finished processing
 * @param outputOperationInfos The output operations in this batch
 */
@DeveloperApi
case class BatchInfo(
    batchTime: Time,
    streamIdToInputInfo: Map[Int, StreamInputInfo],
    submissionTime: Long,
    processingStartTime: Option[Long],
    processingEndTime: Option[Long],
    outputOperationInfos: Map[Int, OutputOperationInfo]
  ) {

  /**
   * Time taken for the first job of this batch to start processing from the time this batch
   * was submitted to the streaming scheduler. Essentially, it is
   * `processingStartTime` - `submissionTime`.
   */
  def schedulingDelay: Option[Long] = processingStartTime.map(_ - submissionTime)

  /**
   * Time taken for the all jobs of this batch to finish processing from the time they started
   * processing. Essentially, it is `processingEndTime` - `processingStartTime`.
   */
  def processingDelay: Option[Long] = processingEndTime.zip(processingStartTime)
    .map(x => x._1 - x._2).headOption

  /**
   * Time taken for all the jobs of this batch to finish processing from the time they
   * were submitted.  Essentially, it is `processingDelay` + `schedulingDelay`.
   */
  def totalDelay: Option[Long] = schedulingDelay.zip(processingDelay)
    .map(x => x._1 + x._2).headOption

  /**
   * The number of recorders received by the receivers in this batch.
   */
  def numRecords: Long = streamIdToInputInfo.values.map(_.numRecords).sum

}
