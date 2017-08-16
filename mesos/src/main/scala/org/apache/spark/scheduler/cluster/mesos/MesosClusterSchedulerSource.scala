/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.scheduler.cluster.mesos

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source

private[mesos] class MesosClusterSchedulerSource(scheduler: MesosClusterScheduler)
  extends Source {
  override def sourceName: String = "mesos_cluster"
  override def metricRegistry: MetricRegistry = new MetricRegistry()

  metricRegistry.register(MetricRegistry.name("waitingDrivers"), new Gauge[Int] {
    override def getValue: Int = scheduler.getQueuedDriversSize
  })

  metricRegistry.register(MetricRegistry.name("launchedDrivers"), new Gauge[Int] {
    override def getValue: Int = scheduler.getLaunchedDriversSize
  })

  metricRegistry.register(MetricRegistry.name("retryDrivers"), new Gauge[Int] {
    override def getValue: Int = scheduler.getPendingRetryDriversSize
  })
}
