/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.rdd

import scala.collection.mutable

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Partition

/**
 * ::DeveloperApi::
 * A PartitionCoalescer defines how to coalesce the partitions of a given RDD.
 */
@DeveloperApi
trait PartitionCoalescer {

  /**
   * Coalesce the partitions of the given RDD.
   *
   * @param maxPartitions the maximum number of partitions to have after coalescing
   * @param parent the parent RDD whose partitions to coalesce
   * @return an array of [[PartitionGroup]]s, where each element is itself an array of
   * `Partition`s and represents a partition after coalescing is performed.
   */
  def coalesce(maxPartitions: Int, parent: RDD[_]): Array[PartitionGroup]
}

/**
 * ::DeveloperApi::
 * A group of `Partition`s
 * @param prefLoc preferred location for the partition group
 */
@DeveloperApi
class PartitionGroup(val prefLoc: Option[String] = None) {
  val partitions = mutable.ArrayBuffer[Partition]()
  def numPartitions: Int = partitions.size
}
