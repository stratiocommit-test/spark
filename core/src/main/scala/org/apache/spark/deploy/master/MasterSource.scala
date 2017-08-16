/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy.master

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source

private[spark] class MasterSource(val master: Master) extends Source {
  override val metricRegistry = new MetricRegistry()
  override val sourceName = "master"

  // Gauge for worker numbers in cluster
  metricRegistry.register(MetricRegistry.name("workers"), new Gauge[Int] {
    override def getValue: Int = master.workers.size
  })

  // Gauge for alive worker numbers in cluster
  metricRegistry.register(MetricRegistry.name("aliveWorkers"), new Gauge[Int]{
    override def getValue: Int = master.workers.count(_.state == WorkerState.ALIVE)
  })

  // Gauge for application numbers in cluster
  metricRegistry.register(MetricRegistry.name("apps"), new Gauge[Int] {
    override def getValue: Int = master.apps.size
  })

  // Gauge for waiting application numbers in cluster
  metricRegistry.register(MetricRegistry.name("waitingApps"), new Gauge[Int] {
    override def getValue: Int = master.apps.count(_.state == ApplicationState.WAITING)
  })
}
