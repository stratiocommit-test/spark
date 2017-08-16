/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.partial

import org.apache.spark.SparkFunSuite

class CountEvaluatorSuite extends SparkFunSuite {

  test("test count 0") {
    val evaluator = new CountEvaluator(10, 0.95)
    assert(new BoundedDouble(0.0, 0.0, 0.0, Double.PositiveInfinity) == evaluator.currentResult())
    evaluator.merge(1, 0)
    assert(new BoundedDouble(0.0, 0.0, 0.0, Double.PositiveInfinity) == evaluator.currentResult())
  }

  test("test count >= 1") {
    val evaluator = new CountEvaluator(10, 0.95)
    evaluator.merge(1, 1)
    assert(new BoundedDouble(10.0, 0.95, 1.0, 36.0) == evaluator.currentResult())
    evaluator.merge(1, 3)
    assert(new BoundedDouble(20.0, 0.95, 7.0, 41.0) == evaluator.currentResult())
    evaluator.merge(1, 8)
    assert(new BoundedDouble(40.0, 0.95, 24.0, 61.0) == evaluator.currentResult())
    (4 to 10).foreach(_ => evaluator.merge(1, 10))
    assert(new BoundedDouble(82.0, 1.0, 82.0, 82.0) == evaluator.currentResult())
  }

}
