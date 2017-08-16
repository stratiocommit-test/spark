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

import org.apache.spark.{SharedSparkContext, SparkFunSuite}

object ZippedPartitionsSuite {
  def procZippedData(i: Iterator[Int], s: Iterator[String], d: Iterator[Double]) : Iterator[Int] = {
    Iterator(i.toArray.size, s.toArray.size, d.toArray.size)
  }
}

class ZippedPartitionsSuite extends SparkFunSuite with SharedSparkContext {
  test("print sizes") {
    val data1 = sc.makeRDD(Array(1, 2, 3, 4), 2)
    val data2 = sc.makeRDD(Array("1", "2", "3", "4", "5", "6"), 2)
    val data3 = sc.makeRDD(Array(1.0, 2.0), 2)

    val zippedRDD = data1.zipPartitions(data2, data3)(ZippedPartitionsSuite.procZippedData)

    val obtainedSizes = zippedRDD.collect()
    val expectedSizes = Array(2, 3, 1, 2, 3, 1)
    assert(obtainedSizes.size == 6)
    assert(obtainedSizes.zip(expectedSizes).forall(x => x._1 == x._2))
  }
}
