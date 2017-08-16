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

import java.{util => ju}

import scala.collection.mutable

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.{Source => CodahaleSource}
import org.apache.spark.util.Clock

/**
 * Serves metrics from a [[org.apache.spark.sql.streaming.StreamingQuery]] to
 * Codahale/DropWizard metrics
 */
class MetricsReporter(
    stream: StreamExecution,
    override val sourceName: String) extends CodahaleSource with Logging {

  override val metricRegistry: MetricRegistry = new MetricRegistry

  // Metric names should not have . in them, so that all the metrics of a query are identified
  // together in Ganglia as a single metric group
  registerGauge("inputRate-total", () => stream.lastProgress.inputRowsPerSecond)
  registerGauge("processingRate-total", () => stream.lastProgress.inputRowsPerSecond)
  registerGauge("latency", () => stream.lastProgress.durationMs.get("triggerExecution").longValue())

  private def registerGauge[T](name: String, f: () => T)(implicit num: Numeric[T]): Unit = {
    synchronized {
      metricRegistry.register(name, new Gauge[T] {
        override def getValue: T = f()
      })
    }
  }
}
