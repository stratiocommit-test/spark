/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.tuning

import scala.collection.mutable

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.{ParamMap, TestParams}

class ParamGridBuilderSuite extends SparkFunSuite {

  val solver = new TestParams()
  import solver.{inputCol, maxIter}

  test("param grid builder") {
    def validateGrid(maps: Array[ParamMap], expected: mutable.Set[(Int, String)]): Unit = {
      assert(maps.size === expected.size)
      maps.foreach { m =>
        val tuple = (m(maxIter), m(inputCol))
        assert(expected.contains(tuple))
        expected.remove(tuple)
      }
      assert(expected.isEmpty)
    }

    val maps0 = new ParamGridBuilder()
      .baseOn(maxIter -> 10)
      .addGrid(inputCol, Array("input0", "input1"))
      .build()
    val expected0 = mutable.Set(
      (10, "input0"),
      (10, "input1"))
    validateGrid(maps0, expected0)

    val maps1 = new ParamGridBuilder()
      .baseOn(ParamMap(maxIter -> 5, inputCol -> "input")) // will be overwritten
      .addGrid(maxIter, Array(10, 20))
      .addGrid(inputCol, Array("input0", "input1"))
      .build()
    val expected1 = mutable.Set(
      (10, "input0"),
      (20, "input0"),
      (10, "input1"),
      (20, "input1"))
    validateGrid(maps1, expected1)
  }
}
