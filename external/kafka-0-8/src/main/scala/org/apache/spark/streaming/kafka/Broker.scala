/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.kafka

import org.apache.spark.annotation.Experimental

/**
 * Represents the host and port info for a Kafka broker.
 * Differs from the Kafka project's internal kafka.cluster.Broker, which contains a server ID.
 */
final class Broker private(
    /** Broker's hostname */
    val host: String,
    /** Broker's port */
    val port: Int) extends Serializable {
  override def equals(obj: Any): Boolean = obj match {
    case that: Broker =>
      this.host == that.host &&
      this.port == that.port
    case _ => false
  }

  override def hashCode: Int = {
    41 * (41 + host.hashCode) + port
  }

  override def toString(): String = {
    s"Broker($host, $port)"
  }
}

/**
 * :: Experimental ::
 * Companion object that provides methods to create instances of [[Broker]].
 */
@Experimental
object Broker {
  def create(host: String, port: Int): Broker =
    new Broker(host, port)

  def apply(host: String, port: Int): Broker =
    new Broker(host, port)

  def unapply(broker: Broker): Option[(String, Int)] = {
    if (broker == null) {
      None
    } else {
      Some((broker.host, broker.port))
    }
  }
}
