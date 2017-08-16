/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.repl

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.util.SignalUtils

private[repl] object Signaling extends Logging {

  /**
   * Register a SIGINT handler, that terminates all active spark jobs or terminates
   * when no jobs are currently running.
   * This makes it possible to interrupt a running shell job by pressing Ctrl+C.
   */
  def cancelOnInterrupt(ctx: SparkContext): Unit = SignalUtils.register("INT") {
    if (!ctx.statusTracker.getActiveJobIds().isEmpty) {
      logWarning("Cancelling all active jobs, this can take a while. " +
        "Press Ctrl+C again to exit now.")
      ctx.cancelAllJobs()
      true
    } else {
      false
    }
  }

}
