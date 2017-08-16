/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.tree

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.tree.impurity.{EntropyAggregator, GiniAggregator}

/**
 * Test suites for [[GiniAggregator]] and [[EntropyAggregator]].
 */
class ImpuritySuite extends SparkFunSuite {
  test("Gini impurity does not support negative labels") {
    val gini = new GiniAggregator(2)
    intercept[IllegalArgumentException] {
      gini.update(Array(0.0, 1.0, 2.0), 0, -1, 0.0)
    }
  }

  test("Entropy does not support negative labels") {
    val entropy = new EntropyAggregator(2)
    intercept[IllegalArgumentException] {
      entropy.update(Array(0.0, 1.0, 2.0), 0, -1, 0.0)
    }
  }
}
