/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source
import org.apache.spark.streaming.ui.StreamingJobProgressListener

private[streaming] class StreamingSource(ssc: StreamingContext) extends Source {
  override val metricRegistry = new MetricRegistry
  override val sourceName = "%s.StreamingMetrics".format(ssc.sparkContext.appName)

  private val streamingListener = ssc.progressListener

  private def registerGauge[T](name: String, f: StreamingJobProgressListener => T,
      defaultValue: T): Unit = {
    registerGaugeWithOption[T](name,
      (l: StreamingJobProgressListener) => Option(f(streamingListener)), defaultValue)
  }

  private def registerGaugeWithOption[T](
      name: String,
      f: StreamingJobProgressListener => Option[T],
      defaultValue: T): Unit = {
    metricRegistry.register(MetricRegistry.name("streaming", name), new Gauge[T] {
      override def getValue: T = f(streamingListener).getOrElse(defaultValue)
    })
  }

  // Gauge for number of network receivers
  registerGauge("receivers", _.numReceivers, 0)

  // Gauge for number of total completed batches
  registerGauge("totalCompletedBatches", _.numTotalCompletedBatches, 0L)

  // Gauge for number of total received records
  registerGauge("totalReceivedRecords", _.numTotalReceivedRecords, 0L)

  // Gauge for number of total processed records
  registerGauge("totalProcessedRecords", _.numTotalProcessedRecords, 0L)

  // Gauge for number of unprocessed batches
  registerGauge("unprocessedBatches", _.numUnprocessedBatches, 0L)

  // Gauge for number of waiting batches
  registerGauge("waitingBatches", _.waitingBatches.size, 0L)

  // Gauge for number of running batches
  registerGauge("runningBatches", _.runningBatches.size, 0L)

  // Gauge for number of retained completed batches
  registerGauge("retainedCompletedBatches", _.retainedCompletedBatches.size, 0L)

  // Gauge for last completed batch, useful for monitoring the streaming job's running status,
  // displayed data -1 for any abnormal condition.
  registerGaugeWithOption("lastCompletedBatch_submissionTime",
    _.lastCompletedBatch.map(_.submissionTime), -1L)
  registerGaugeWithOption("lastCompletedBatch_processingStartTime",
    _.lastCompletedBatch.flatMap(_.processingStartTime), -1L)
  registerGaugeWithOption("lastCompletedBatch_processingEndTime",
    _.lastCompletedBatch.flatMap(_.processingEndTime), -1L)

  // Gauge for last completed batch's delay information.
  registerGaugeWithOption("lastCompletedBatch_processingDelay",
    _.lastCompletedBatch.flatMap(_.processingDelay), -1L)
  registerGaugeWithOption("lastCompletedBatch_schedulingDelay",
    _.lastCompletedBatch.flatMap(_.schedulingDelay), -1L)
  registerGaugeWithOption("lastCompletedBatch_totalDelay",
    _.lastCompletedBatch.flatMap(_.totalDelay), -1L)

  // Gauge for last received batch, useful for monitoring the streaming job's running status,
  // displayed data -1 for any abnormal condition.
  registerGaugeWithOption("lastReceivedBatch_submissionTime",
    _.lastReceivedBatch.map(_.submissionTime), -1L)
  registerGaugeWithOption("lastReceivedBatch_processingStartTime",
    _.lastReceivedBatch.flatMap(_.processingStartTime), -1L)
  registerGaugeWithOption("lastReceivedBatch_processingEndTime",
    _.lastReceivedBatch.flatMap(_.processingEndTime), -1L)

  // Gauge for last received batch records.
  registerGauge("lastReceivedBatch_records", _.lastReceivedBatchRecords.values.sum, 0L)
}
