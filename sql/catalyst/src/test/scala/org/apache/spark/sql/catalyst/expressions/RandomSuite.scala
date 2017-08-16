/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.expressions

import org.scalatest.Matchers._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.{IntegerType, LongType}

class RandomSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("random") {
    checkDoubleEvaluation(Rand(30), 0.31429268272540556 +- 0.001)
    checkDoubleEvaluation(Randn(30), -0.4798519469521663 +- 0.001)

    checkDoubleEvaluation(
      new Rand(Literal.create(null, LongType)), 0.8446490682263027 +- 0.001)
    checkDoubleEvaluation(
      new Randn(Literal.create(null, IntegerType)), 1.1164209726833079 +- 0.001)
  }

  test("SPARK-9127 codegen with long seed") {
    checkDoubleEvaluation(Rand(5419823303878592871L), 0.2304755080444375 +- 0.001)
    checkDoubleEvaluation(Randn(5419823303878592871L), -1.2824262718225607 +- 0.001)
  }
}
