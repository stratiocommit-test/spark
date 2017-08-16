/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.scheduler

import java.util.Arrays
import java.util.Objects

import org.apache.spark._
import org.apache.spark.rdd.RDD

/**
 * A Partitioner that might group together one or more partitions from the parent.
 *
 * @param parent a parent partitioner
 * @param partitionStartIndices indices of partitions in parent that should create new partitions
 *   in child (this should be an array of increasing partition IDs). For example, if we have a
 *   parent with 5 partitions, and partitionStartIndices is [0, 2, 4], we get three output
 *   partitions, corresponding to partition ranges [0, 1], [2, 3] and [4] of the parent partitioner.
 */
class CoalescedPartitioner(val parent: Partitioner, val partitionStartIndices: Array[Int])
  extends Partitioner {

  @transient private lazy val parentPartitionMapping: Array[Int] = {
    val n = parent.numPartitions
    val result = new Array[Int](n)
    for (i <- 0 until partitionStartIndices.length) {
      val start = partitionStartIndices(i)
      val end = if (i < partitionStartIndices.length - 1) partitionStartIndices(i + 1) else n
      for (j <- start until end) {
        result(j) = i
      }
    }
    result
  }

  override def numPartitions: Int = partitionStartIndices.size

  override def getPartition(key: Any): Int = {
    parentPartitionMapping(parent.getPartition(key))
  }

  override def hashCode(): Int =
    31 * Objects.hashCode(parent) + Arrays.hashCode(partitionStartIndices)

  override def equals(other: Any): Boolean = other match {
    case c: CoalescedPartitioner =>
      c.parent == parent && Arrays.equals(c.partitionStartIndices, partitionStartIndices)
    case _ =>
      false
  }
}

private[spark] class CustomShuffledRDDPartition(
    val index: Int, val startIndexInParent: Int, val endIndexInParent: Int)
  extends Partition {

  override def hashCode(): Int = index

  override def equals(other: Any): Boolean = super.equals(other)
}

/**
 * A special ShuffledRDD that supports a ShuffleDependency object from outside and launching reduce
 * tasks that read multiple map output partitions.
 */
class CustomShuffledRDD[K, V, C](
    var dependency: ShuffleDependency[K, V, C],
    partitionStartIndices: Array[Int])
  extends RDD[(K, C)](dependency.rdd.context, Seq(dependency)) {

  def this(dep: ShuffleDependency[K, V, C]) = {
    this(dep, (0 until dep.partitioner.numPartitions).toArray)
  }

  override def getDependencies: Seq[Dependency[_]] = List(dependency)

  override val partitioner = {
    Some(new CoalescedPartitioner(dependency.partitioner, partitionStartIndices))
  }

  override def getPartitions: Array[Partition] = {
    val n = dependency.partitioner.numPartitions
    Array.tabulate[Partition](partitionStartIndices.length) { i =>
      val startIndex = partitionStartIndices(i)
      val endIndex = if (i < partitionStartIndices.length - 1) partitionStartIndices(i + 1) else n
      new CustomShuffledRDDPartition(i, startIndex, endIndex)
    }
  }

  override def compute(p: Partition, context: TaskContext): Iterator[(K, C)] = {
    val part = p.asInstanceOf[CustomShuffledRDDPartition]
    SparkEnv.get.shuffleManager.getReader(
      dependency.shuffleHandle, part.startIndexInParent, part.endIndexInParent, context)
      .read()
      .asInstanceOf[Iterator[(K, C)]]
  }

  override def clearDependencies() {
    super.clearDependencies()
    dependency = null
  }
}
