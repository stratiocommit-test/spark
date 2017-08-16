/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

class ReduceAggregatorSuite extends SparkFunSuite {

  test("zero value") {
    val encoder: ExpressionEncoder[Int] = ExpressionEncoder()
    val func = (v1: Int, v2: Int) => v1 + v2
    val aggregator: ReduceAggregator[Int] = new ReduceAggregator(func)(Encoders.scalaInt)
    assert(aggregator.zero == (false, null))
  }

  test("reduce, merge and finish") {
    val encoder: ExpressionEncoder[Int] = ExpressionEncoder()
    val func = (v1: Int, v2: Int) => v1 + v2
    val aggregator: ReduceAggregator[Int] = new ReduceAggregator(func)(Encoders.scalaInt)

    val firstReduce = aggregator.reduce(aggregator.zero, 1)
    assert(firstReduce == (true, 1))

    val secondReduce = aggregator.reduce(firstReduce, 2)
    assert(secondReduce == (true, 3))

    val thirdReduce = aggregator.reduce(secondReduce, 3)
    assert(thirdReduce == (true, 6))

    val mergeWithZero1 = aggregator.merge(aggregator.zero, firstReduce)
    assert(mergeWithZero1 == (true, 1))

    val mergeWithZero2 = aggregator.merge(secondReduce, aggregator.zero)
    assert(mergeWithZero2 == (true, 3))

    val mergeTwoReduced = aggregator.merge(firstReduce, secondReduce)
    assert(mergeTwoReduced == (true, 4))

    assert(aggregator.finish(firstReduce)== 1)
    assert(aggregator.finish(secondReduce) == 3)
    assert(aggregator.finish(thirdReduce) == 6)
    assert(aggregator.finish(mergeWithZero1) == 1)
    assert(aggregator.finish(mergeWithZero2) == 3)
    assert(aggregator.finish(mergeTwoReduced) == 4)
  }

  test("requires at least one input row") {
    val encoder: ExpressionEncoder[Int] = ExpressionEncoder()
    val func = (v1: Int, v2: Int) => v1 + v2
    val aggregator: ReduceAggregator[Int] = new ReduceAggregator(func)(Encoders.scalaInt)

    intercept[IllegalStateException] {
      aggregator.finish(aggregator.zero)
    }
  }
}
