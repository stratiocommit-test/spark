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

import org.scalatest.exceptions.TestFailedException

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row

class PolynomialExpansionSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new PolynomialExpansion)
  }

  private val data = Array(
    Vectors.sparse(3, Seq((0, -2.0), (1, 2.3))),
    Vectors.dense(-2.0, 2.3),
    Vectors.dense(0.0, 0.0, 0.0),
    Vectors.dense(0.6, -1.1, -3.0),
    Vectors.sparse(3, Seq())
  )

  private val twoDegreeExpansion: Array[Vector] = Array(
    Vectors.sparse(9, Array(0, 1, 2, 3, 4), Array(-2.0, 4.0, 2.3, -4.6, 5.29)),
    Vectors.dense(-2.0, 4.0, 2.3, -4.6, 5.29),
    Vectors.dense(new Array[Double](9)),
    Vectors.dense(0.6, 0.36, -1.1, -0.66, 1.21, -3.0, -1.8, 3.3, 9.0),
    Vectors.sparse(9, Array.empty, Array.empty))

  private val threeDegreeExpansion: Array[Vector] = Array(
    Vectors.sparse(19, Array(0, 1, 2, 3, 4, 5, 6, 7, 8),
      Array(-2.0, 4.0, -8.0, 2.3, -4.6, 9.2, 5.29, -10.58, 12.17)),
    Vectors.dense(-2.0, 4.0, -8.0, 2.3, -4.6, 9.2, 5.29, -10.58, 12.17),
    Vectors.dense(new Array[Double](19)),
    Vectors.dense(0.6, 0.36, 0.216, -1.1, -0.66, -0.396, 1.21, 0.726, -1.331, -3.0, -1.8,
      -1.08, 3.3, 1.98, -3.63, 9.0, 5.4, -9.9, -27.0),
    Vectors.sparse(19, Array.empty, Array.empty))

  test("Polynomial expansion with default parameter") {
    val df = data.zip(twoDegreeExpansion).toSeq.toDF("features", "expected")

    val polynomialExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")

    polynomialExpansion.transform(df).select("polyFeatures", "expected").collect().foreach {
      case Row(expanded: DenseVector, expected: DenseVector) =>
        assert(expanded ~== expected absTol 1e-1)
      case Row(expanded: SparseVector, expected: SparseVector) =>
        assert(expanded ~== expected absTol 1e-1)
      case _ =>
        throw new TestFailedException("Unmatched data types after polynomial expansion", 0)
    }
  }

  test("Polynomial expansion with setter") {
    val df = data.zip(threeDegreeExpansion).toSeq.toDF("features", "expected")

    val polynomialExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")
      .setDegree(3)

    polynomialExpansion.transform(df).select("polyFeatures", "expected").collect().foreach {
      case Row(expanded: DenseVector, expected: DenseVector) =>
        assert(expanded ~== expected absTol 1e-1)
      case Row(expanded: SparseVector, expected: SparseVector) =>
        assert(expanded ~== expected absTol 1e-1)
      case _ =>
        throw new TestFailedException("Unmatched data types after polynomial expansion", 0)
    }
  }

  test("Polynomial expansion with degree 1 is identity on vectors") {
    val df = data.zip(data).toSeq.toDF("features", "expected")

    val polynomialExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")
      .setDegree(1)

    polynomialExpansion.transform(df).select("polyFeatures", "expected").collect().foreach {
      case Row(expanded: Vector, expected: Vector) =>
        assert(expanded ~== expected absTol 1e-1)
      case _ =>
        throw new TestFailedException("Unmatched data types after polynomial expansion", 0)
    }
  }

  test("read/write") {
    val t = new PolynomialExpansion()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setDegree(3)
    testDefaultReadWrite(t)
  }

  test("SPARK-17027. Integer overflow in PolynomialExpansion.getPolySize") {
    val data: Array[(Vector, Int, Int)] = Array(
      (Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0), 3002, 4367),
      (Vectors.sparse(5, Seq((0, 1.0), (4, 5.0))), 3002, 4367),
      (Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0), 8007, 12375)
    )

    val df = data.toSeq.toDF("features", "expectedPoly10size", "expectedPoly11size")

    val t = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")

    for (i <- Seq(10, 11)) {
      val transformed = t.setDegree(i)
        .transform(df)
        .select(s"expectedPoly${i}size", "polyFeatures")
        .rdd.map { case Row(expected: Int, v: Vector) => expected == v.size }

      assert(transformed.collect.forall(identity))
    }
  }
}

