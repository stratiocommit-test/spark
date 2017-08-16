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

import org.apache.spark.Partition

/**
 * @param topic kafka topic name
 * @param partition kafka partition id
 * @param fromOffset inclusive starting offset
 * @param untilOffset exclusive ending offset
 * @param host preferred kafka host, i.e. the leader at the time the rdd was created
 * @param port preferred kafka host's port
 */
private[kafka]
class KafkaRDDPartition(
  val index: Int,
  val topic: String,
  val partition: Int,
  val fromOffset: Long,
  val untilOffset: Long,
  val host: String,
  val port: Int
) extends Partition {
  /** Number of messages this partition refers to */
  def count(): Long = untilOffset - fromOffset
}
