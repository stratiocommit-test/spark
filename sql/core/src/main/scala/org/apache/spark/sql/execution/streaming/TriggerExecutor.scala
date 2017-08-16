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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.util.{Clock, SystemClock}

trait TriggerExecutor {

  /**
   * Execute batches using `batchRunner`. If `batchRunner` runs `false`, terminate the execution.
   */
  def execute(batchRunner: () => Boolean): Unit
}

/**
 * A trigger executor that runs a batch every `intervalMs` milliseconds.
 */
case class ProcessingTimeExecutor(processingTime: ProcessingTime, clock: Clock = new SystemClock())
  extends TriggerExecutor with Logging {

  private val intervalMs = processingTime.intervalMs

  override def execute(batchRunner: () => Boolean): Unit = {
    while (true) {
      val batchStartTimeMs = clock.getTimeMillis()
      val terminated = !batchRunner()
      if (intervalMs > 0) {
        val batchEndTimeMs = clock.getTimeMillis()
        val batchElapsedTimeMs = batchEndTimeMs - batchStartTimeMs
        if (batchElapsedTimeMs > intervalMs) {
          notifyBatchFallingBehind(batchElapsedTimeMs)
        }
        if (terminated) {
          return
        }
        clock.waitTillTime(nextBatchTime(batchEndTimeMs))
      } else {
        if (terminated) {
          return
        }
      }
    }
  }

  /** Called when a batch falls behind. Expose for test only */
  def notifyBatchFallingBehind(realElapsedTimeMs: Long): Unit = {
    logWarning("Current batch is falling behind. The trigger interval is " +
      s"${intervalMs} milliseconds, but spent ${realElapsedTimeMs} milliseconds")
  }

  /**
   * Returns the start time in milliseconds for the next batch interval, given the current time.
   * Note that a batch interval is inclusive with respect to its start time, and thus calling
   * `nextBatchTime` with the result of a previous call should return the next interval. (i.e. given
   * an interval of `100 ms`, `nextBatchTime(nextBatchTime(0)) = 200` rather than `0`).
   */
  def nextBatchTime(now: Long): Long = {
    now / intervalMs * intervalMs + intervalMs
  }
}
