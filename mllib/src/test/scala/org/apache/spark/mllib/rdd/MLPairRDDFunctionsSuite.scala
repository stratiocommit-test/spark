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
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import org.apache.spark.mllib.util.MLlibTestSparkContext

class MLPairRDDFunctionsSuite extends SparkFunSuite with MLlibTestSparkContext {
  test("topByKey") {
    val topMap = sc.parallelize(Array((1, 7), (1, 3), (1, 6), (1, 1), (1, 2), (3, 2), (3, 7), (5,
      1), (3, 5)), 2)
      .topByKey(5)
      .collectAsMap()

    assert(topMap.size === 3)
    assert(topMap(1) === Array(7, 6, 3, 2, 1))
    assert(topMap(3) === Array(7, 5, 2))
    assert(topMap(5) === Array(1))
  }
}
