/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.ganglia.GangliaReporter
import info.ganglia.gmetric4j.gmetric.GMetric
import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode

import org.apache.spark.SecurityManager
import org.apache.spark.metrics.MetricsSystem

class GangliaSink(val property: Properties, val registry: MetricRegistry,
    securityMgr: SecurityManager) extends Sink {
  val GANGLIA_KEY_PERIOD = "period"
  val GANGLIA_DEFAULT_PERIOD = 10

  val GANGLIA_KEY_UNIT = "unit"
  val GANGLIA_DEFAULT_UNIT: TimeUnit = TimeUnit.SECONDS

  val GANGLIA_KEY_MODE = "mode"
  val GANGLIA_DEFAULT_MODE: UDPAddressingMode = GMetric.UDPAddressingMode.MULTICAST

  // TTL for multicast messages. If listeners are X hops away in network, must be at least X.
  val GANGLIA_KEY_TTL = "ttl"
  val GANGLIA_DEFAULT_TTL = 1

  val GANGLIA_KEY_HOST = "host"
  val GANGLIA_KEY_PORT = "port"

  val GANGLIA_KEY_DMAX = "dmax"
  val GANGLIA_DEFAULT_DMAX = 0

  def propertyToOption(prop: String): Option[String] = Option(property.getProperty(prop))

  if (!propertyToOption(GANGLIA_KEY_HOST).isDefined) {
    throw new Exception("Ganglia sink requires 'host' property.")
  }

  if (!propertyToOption(GANGLIA_KEY_PORT).isDefined) {
    throw new Exception("Ganglia sink requires 'port' property.")
  }

  val host = propertyToOption(GANGLIA_KEY_HOST).get
  val port = propertyToOption(GANGLIA_KEY_PORT).get.toInt
  val ttl = propertyToOption(GANGLIA_KEY_TTL).map(_.toInt).getOrElse(GANGLIA_DEFAULT_TTL)
  val dmax = propertyToOption(GANGLIA_KEY_DMAX).map(_.toInt).getOrElse(GANGLIA_DEFAULT_DMAX)
  val mode: UDPAddressingMode = propertyToOption(GANGLIA_KEY_MODE)
    .map(u => GMetric.UDPAddressingMode.valueOf(u.toUpperCase)).getOrElse(GANGLIA_DEFAULT_MODE)
  val pollPeriod = propertyToOption(GANGLIA_KEY_PERIOD).map(_.toInt)
    .getOrElse(GANGLIA_DEFAULT_PERIOD)
  val pollUnit: TimeUnit = propertyToOption(GANGLIA_KEY_UNIT)
    .map(u => TimeUnit.valueOf(u.toUpperCase))
    .getOrElse(GANGLIA_DEFAULT_UNIT)

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val ganglia = new GMetric(host, port, mode, ttl)
  val reporter: GangliaReporter = GangliaReporter.forRegistry(registry)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .withDMax(dmax)
      .build(ganglia)

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }

  override def report() {
    reporter.report()
  }
}

