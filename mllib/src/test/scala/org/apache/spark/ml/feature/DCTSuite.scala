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

import scala.beans.BeanInfo

import edu.emory.mathcs.jtransforms.dct.DoubleDCT_1D

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row

@BeanInfo
case class DCTTestData(vec: Vector, wantedVec: Vector)

class DCTSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("forward transform of discrete cosine matches jTransforms result") {
    val data = Vectors.dense((0 until 128).map(_ => 2D * math.random - 1D).toArray)
    val inverse = false

    testDCT(data, inverse)
  }

  test("inverse transform of discrete cosine matches jTransforms result") {
    val data = Vectors.dense((0 until 128).map(_ => 2D * math.random - 1D).toArray)
    val inverse = true

    testDCT(data, inverse)
  }

  test("read/write") {
    val t = new DCT()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setInverse(true)
    testDefaultReadWrite(t)
  }

  private def testDCT(data: Vector, inverse: Boolean): Unit = {
    val expectedResultBuffer = data.toArray.clone()
    if (inverse) {
      new DoubleDCT_1D(data.size).inverse(expectedResultBuffer, true)
    } else {
      new DoubleDCT_1D(data.size).forward(expectedResultBuffer, true)
    }
    val expectedResult = Vectors.dense(expectedResultBuffer)

    val dataset = Seq(DCTTestData(data, expectedResult)).toDF()

    val transformer = new DCT()
      .setInputCol("vec")
      .setOutputCol("resultVec")
      .setInverse(inverse)

    transformer.transform(dataset)
      .select("resultVec", "wantedVec")
      .collect()
      .foreach { case Row(resultVec: Vector, wantedVec: Vector) =>
      assert(Vectors.sqdist(resultVec, wantedVec) < 1e-6)
    }
  }
}
