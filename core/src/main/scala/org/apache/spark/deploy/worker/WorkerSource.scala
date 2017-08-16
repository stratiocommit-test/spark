/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy.worker

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source

private[worker] class WorkerSource(val worker: Worker) extends Source {
  override val sourceName = "worker"
  override val metricRegistry = new MetricRegistry()

  metricRegistry.register(MetricRegistry.name("executors"), new Gauge[Int] {
    override def getValue: Int = worker.executors.size
  })

  // Gauge for cores used of this worker
  metricRegistry.register(MetricRegistry.name("coresUsed"), new Gauge[Int] {
    override def getValue: Int = worker.coresUsed
  })

  // Gauge for memory used of this worker
  metricRegistry.register(MetricRegistry.name("memUsed_MB"), new Gauge[Int] {
    override def getValue: Int = worker.memoryUsed
  })

  // Gauge for cores free of this worker
  metricRegistry.register(MetricRegistry.name("coresFree"), new Gauge[Int] {
    override def getValue: Int = worker.coresFree
  })

  // Gauge for memory free of this worker
  metricRegistry.register(MetricRegistry.name("memFree_MB"), new Gauge[Int] {
    override def getValue: Int = worker.memoryFree
  })
}
