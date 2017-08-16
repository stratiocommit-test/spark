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

import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Encoder, ForeachWriter}
import org.apache.spark.sql.catalyst.encoders.encoderFor

/**
 * A [[Sink]] that forwards all data into [[ForeachWriter]] according to the contract defined by
 * [[ForeachWriter]].
 *
 * @param writer The [[ForeachWriter]] to process all data.
 * @tparam T The expected type of the sink.
 */
class ForeachSink[T : Encoder](writer: ForeachWriter[T]) extends Sink with Serializable {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    // This logic should've been as simple as:
    // ```
    //   data.as[T].foreachPartition { iter => ... }
    // ```
    //
    // Unfortunately, doing that would just break the incremental planing. The reason is,
    // `Dataset.foreachPartition()` would further call `Dataset.rdd()`, but `Dataset.rdd()` will
    // create a new plan. Because StreamExecution uses the existing plan to collect metrics and
    // update watermark, we should never create a new plan. Otherwise, metrics and watermark are
    // updated in the new plan, and StreamExecution cannot retrieval them.
    //
    // Hence, we need to manually convert internal rows to objects using encoder.
    val encoder = encoderFor[T].resolveAndBind(
      data.logicalPlan.output,
      data.sparkSession.sessionState.analyzer)
    data.queryExecution.toRdd.foreachPartition { iter =>
      if (writer.open(TaskContext.getPartitionId(), batchId)) {
        try {
          while (iter.hasNext) {
            writer.process(encoder.fromRow(iter.next()))
          }
        } catch {
          case e: Throwable =>
            writer.close(e)
            throw e
        }
        writer.close(null)
      } else {
        writer.close(null)
      }
    }
  }
}
