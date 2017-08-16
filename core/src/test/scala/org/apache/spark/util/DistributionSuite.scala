/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.util

import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite

/**
 *
 */

class DistributionSuite extends SparkFunSuite with Matchers {
  test("summary") {
    val d = new Distribution((1 to 100).toArray.map{_.toDouble})
    val stats = d.statCounter
    stats.count should be (100)
    stats.mean should be (50.5)
    stats.sum should be (50 * 101)

    val quantiles = d.getQuantiles()
    quantiles(0) should be (1)
    quantiles(1) should be (26)
    quantiles(2) should be (51)
    quantiles(3) should be (76)
    quantiles(4) should be (100)
  }
}
