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
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.feature.{HashingTF => MLlibHashingTF}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.util.Utils

class HashingTFSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new HashingTF)
  }

  test("hashingTF") {
    val df = Seq((0, "a a b b c d".split(" ").toSeq)).toDF("id", "words")
    val n = 100
    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("features")
      .setNumFeatures(n)
    val output = hashingTF.transform(df)
    val attrGroup = AttributeGroup.fromStructField(output.schema("features"))
    require(attrGroup.numAttributes === Some(n))
    val features = output.select("features").first().getAs[Vector](0)
    // Assume perfect hash on "a", "b", "c", and "d".
    def idx: Any => Int = murmur3FeatureIdx(n)
    val expected = Vectors.sparse(n,
      Seq((idx("a"), 2.0), (idx("b"), 2.0), (idx("c"), 1.0), (idx("d"), 1.0)))
    assert(features ~== expected absTol 1e-14)
  }

  test("applying binary term freqs") {
    val df = Seq((0, "a a b c c c".split(" ").toSeq)).toDF("id", "words")
    val n = 100
    val hashingTF = new HashingTF()
        .setInputCol("words")
        .setOutputCol("features")
        .setNumFeatures(n)
        .setBinary(true)
    val output = hashingTF.transform(df)
    val features = output.select("features").first().getAs[Vector](0)
    def idx: Any => Int = murmur3FeatureIdx(n)  // Assume perfect hash on input features
    val expected = Vectors.sparse(n,
      Seq((idx("a"), 1.0), (idx("b"), 1.0), (idx("c"), 1.0)))
    assert(features ~== expected absTol 1e-14)
  }

  test("read/write") {
    val t = new HashingTF()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setNumFeatures(10)
    testDefaultReadWrite(t)
  }

  private def murmur3FeatureIdx(numFeatures: Int)(term: Any): Int = {
    Utils.nonNegativeMod(MLlibHashingTF.murmur3Hash(term), numFeatures)
  }
}
