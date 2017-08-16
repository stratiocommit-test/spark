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
import org.apache.spark.util.StatCounter

class MeanEvaluatorSuite extends SparkFunSuite {

  test("test count 0") {
    val evaluator = new MeanEvaluator(10, 0.95)
    assert(new BoundedDouble(0.0, 0.0, Double.NegativeInfinity, Double.PositiveInfinity) ==
      evaluator.currentResult())
    evaluator.merge(1, new StatCounter())
    assert(new BoundedDouble(0.0, 0.0, Double.NegativeInfinity, Double.PositiveInfinity) ==
      evaluator.currentResult())
    evaluator.merge(1, new StatCounter(Seq(0.0)))
    assert(new BoundedDouble(0.0, 0.95, Double.NegativeInfinity, Double.PositiveInfinity) ==
      evaluator.currentResult())
  }

  test("test count 1") {
    val evaluator = new MeanEvaluator(10, 0.95)
    evaluator.merge(1, new StatCounter(Seq(1.0)))
    assert(new BoundedDouble(1.0, 0.95, Double.NegativeInfinity, Double.PositiveInfinity) ==
      evaluator.currentResult())
  }

  test("test count > 1") {
    val evaluator = new MeanEvaluator(10, 0.95)
    evaluator.merge(1, new StatCounter(Seq(1.0)))
    evaluator.merge(1, new StatCounter(Seq(3.0)))
    assert(new BoundedDouble(2.0, 0.95, -10.706204736174746, 14.706204736174746) ==
      evaluator.currentResult())
    evaluator.merge(1, new StatCounter(Seq(8.0)))
    assert(new BoundedDouble(4.0, 0.95, -4.9566858949231225, 12.956685894923123) ==
      evaluator.currentResult())
    (4 to 10).foreach(_ => evaluator.merge(1, new StatCounter(Seq(9.0))))
    assert(new BoundedDouble(7.5, 1.0, 7.5, 7.5) == evaluator.currentResult())
  }

}
