/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.rdd

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.mllib.util.MLlibTestSparkContext

class RDDFunctionsSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("sliding") {
    val data = 0 until 6
    for (numPartitions <- 1 to 8) {
      val rdd = sc.parallelize(data, numPartitions)
      for (windowSize <- 1 to 6) {
        for (step <- 1 to 3) {
          val sliding = rdd.sliding(windowSize, step).collect().map(_.toList).toList
          val expected = data.sliding(windowSize, step)
            .map(_.toList).toList.filter(l => l.size == windowSize)
          assert(sliding === expected)
        }
      }
      assert(rdd.sliding(7).collect().isEmpty,
        "Should return an empty RDD if the window size is greater than the number of items.")
    }
  }

  test("sliding with empty partitions") {
    val data = Seq(Seq(1, 2, 3), Seq.empty[Int], Seq(4), Seq.empty[Int], Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).flatMap(s => s)
    assert(rdd.partitions.length === data.length)
    val sliding = rdd.sliding(3).collect().toSeq.map(_.toSeq)
    val expected = data.flatMap(x => x).sliding(3).toSeq.map(_.toSeq)
    assert(sliding === expected)
  }
}
