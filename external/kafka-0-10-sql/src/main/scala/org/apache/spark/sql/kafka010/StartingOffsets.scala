/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.kafka010

import org.apache.kafka.common.TopicPartition

/*
 * Values that can be specified for config startingOffsets
 */
private[kafka010] sealed trait StartingOffsets

private[kafka010] case object EarliestOffsets extends StartingOffsets

private[kafka010] case object LatestOffsets extends StartingOffsets

private[kafka010] case class SpecificOffsets(
  partitionOffsets: Map[TopicPartition, Long]) extends StartingOffsets
