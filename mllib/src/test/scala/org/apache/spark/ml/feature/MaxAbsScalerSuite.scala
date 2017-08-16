/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row

class MaxAbsScalerSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("MaxAbsScaler fit basic case") {
    val data = Array(
      Vectors.dense(1, 0, 100),
      Vectors.dense(2, 0, 0),
      Vectors.sparse(3, Array(0, 2), Array(-2, -100)),
      Vectors.sparse(3, Array(0), Array(-1.5)))

    val expected: Array[Vector] = Array(
      Vectors.dense(0.5, 0, 1),
      Vectors.dense(1, 0, 0),
      Vectors.sparse(3, Array(0, 2), Array(-1, -1)),
      Vectors.sparse(3, Array(0), Array(-0.75)))

    val df = data.zip(expected).toSeq.toDF("features", "expected")
    val scaler = new MaxAbsScaler()
      .setInputCol("features")
      .setOutputCol("scaled")

    val model = scaler.fit(df)
    model.transform(df).select("expected", "scaled").collect()
      .foreach { case Row(vector1: Vector, vector2: Vector) =>
      assert(vector1.equals(vector2), s"MaxAbsScaler ut error: $vector2 should be $vector1")
    }

    // copied model must have the same parent.
    MLTestingUtils.checkCopy(model)
  }

  test("MaxAbsScaler read/write") {
    val t = new MaxAbsScaler()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
    testDefaultReadWrite(t)
  }

  test("MaxAbsScalerModel read/write") {
    val instance = new MaxAbsScalerModel(
      "myMaxAbsScalerModel", Vectors.dense(1.0, 10.0))
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.maxAbs === instance.maxAbs)
  }

}
