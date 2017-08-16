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
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row}


class NormalizerSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  @transient var data: Array[Vector] = _
  @transient var dataFrame: DataFrame = _
  @transient var normalizer: Normalizer = _
  @transient var l1Normalized: Array[Vector] = _
  @transient var l2Normalized: Array[Vector] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    data = Array(
      Vectors.sparse(3, Seq((0, -2.0), (1, 2.3))),
      Vectors.dense(0.0, 0.0, 0.0),
      Vectors.dense(0.6, -1.1, -3.0),
      Vectors.sparse(3, Seq((1, 0.91), (2, 3.2))),
      Vectors.sparse(3, Seq((0, 5.7), (1, 0.72), (2, 2.7))),
      Vectors.sparse(3, Seq())
    )
    l1Normalized = Array(
      Vectors.sparse(3, Seq((0, -0.465116279), (1, 0.53488372))),
      Vectors.dense(0.0, 0.0, 0.0),
      Vectors.dense(0.12765957, -0.23404255, -0.63829787),
      Vectors.sparse(3, Seq((1, 0.22141119), (2, 0.7785888))),
      Vectors.dense(0.625, 0.07894737, 0.29605263),
      Vectors.sparse(3, Seq())
    )
    l2Normalized = Array(
      Vectors.sparse(3, Seq((0, -0.65617871), (1, 0.75460552))),
      Vectors.dense(0.0, 0.0, 0.0),
      Vectors.dense(0.184549876, -0.3383414, -0.922749378),
      Vectors.sparse(3, Seq((1, 0.27352993), (2, 0.96186349))),
      Vectors.dense(0.897906166, 0.113419726, 0.42532397),
      Vectors.sparse(3, Seq())
    )

    dataFrame = data.map(NormalizerSuite.FeatureData).toSeq.toDF()
    normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normalized_features")
  }

  def collectResult(result: DataFrame): Array[Vector] = {
    result.select("normalized_features").collect().map {
      case Row(features: Vector) => features
    }
  }

  def assertTypeOfVector(lhs: Array[Vector], rhs: Array[Vector]): Unit = {
    assert((lhs, rhs).zipped.forall {
      case (v1: DenseVector, v2: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector) => true
      case _ => false
    }, "The vector type should be preserved after normalization.")
  }

  def assertValues(lhs: Array[Vector], rhs: Array[Vector]): Unit = {
    assert((lhs, rhs).zipped.forall { (vector1, vector2) =>
      vector1 ~== vector2 absTol 1E-5
    }, "The vector value is not correct after normalization.")
  }

  test("Normalization with default parameter") {
    val result = collectResult(normalizer.transform(dataFrame))

    assertTypeOfVector(data, result)

    assertValues(result, l2Normalized)
  }

  test("Normalization with setter") {
    normalizer.setP(1)

    val result = collectResult(normalizer.transform(dataFrame))

    assertTypeOfVector(data, result)

    assertValues(result, l1Normalized)
  }

  test("read/write") {
    val t = new Normalizer()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setP(3.0)
    testDefaultReadWrite(t)
  }
}

private object NormalizerSuite {
  case class FeatureData(features: Vector)
}
