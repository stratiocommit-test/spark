/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy

import javax.annotation.concurrent.ThreadSafe

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler

/**
 * Provides metrics source for external shuffle service
 */
@ThreadSafe
private class ExternalShuffleServiceSource
(blockHandler: ExternalShuffleBlockHandler) extends Source {
  override val metricRegistry = new MetricRegistry()
  override val sourceName = "shuffleService"

  metricRegistry.registerAll(blockHandler.getAllMetrics)
}
