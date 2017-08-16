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

import org.apache.spark.{Partition, SharedSparkContext, SparkFunSuite, TaskContext}

class PartitionPruningRDDSuite extends SparkFunSuite with SharedSparkContext {

  test("Pruned Partitions inherit locality prefs correctly") {

    val rdd = new RDD[Int](sc, Nil) {
      override protected def getPartitions = {
        Array[Partition](
          new TestPartition(0, 1),
          new TestPartition(1, 1),
          new TestPartition(2, 1))
      }

      def compute(split: Partition, context: TaskContext) = {
        Iterator()
      }
    }
    val prunedRDD = PartitionPruningRDD.create(rdd, _ == 2)
    assert(prunedRDD.partitions.length == 1)
    val p = prunedRDD.partitions(0)
    assert(p.index == 0)
    assert(p.asInstanceOf[PartitionPruningRDDPartition].parentSplit.index == 2)
  }


  test("Pruned Partitions can be unioned ") {

    val rdd = new RDD[Int](sc, Nil) {
      override protected def getPartitions = {
        Array[Partition](
          new TestPartition(0, 4),
          new TestPartition(1, 5),
          new TestPartition(2, 6))
      }

      def compute(split: Partition, context: TaskContext) = {
        List(split.asInstanceOf[TestPartition].testValue).iterator
      }
    }
    val prunedRDD1 = PartitionPruningRDD.create(rdd, _ == 0)


    val prunedRDD2 = PartitionPruningRDD.create(rdd, _ == 2)

    val merged = prunedRDD1 ++ prunedRDD2
    assert(merged.count() == 2)
    val take = merged.take(2)
    assert(take.apply(0) == 4)
    assert(take.apply(1) == 6)
  }
}

class TestPartition(i: Int, value: Int) extends Partition with Serializable {
  def index: Int = i
  def testValue: Int = this.value
}
