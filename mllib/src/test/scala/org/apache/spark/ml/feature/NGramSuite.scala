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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{Dataset, Row}

@BeanInfo
case class NGramTestData(inputTokens: Array[String], wantedNGrams: Array[String])

class NGramSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import org.apache.spark.ml.feature.NGramSuite._
  import testImplicits._

  test("default behavior yields bigram features") {
    val nGram = new NGram()
      .setInputCol("inputTokens")
      .setOutputCol("nGrams")
    val dataset = Seq(NGramTestData(
      Array("Test", "for", "ngram", "."),
      Array("Test for", "for ngram", "ngram .")
    )).toDF()
    testNGram(nGram, dataset)
  }

  test("NGramLength=4 yields length 4 n-grams") {
    val nGram = new NGram()
      .setInputCol("inputTokens")
      .setOutputCol("nGrams")
      .setN(4)
    val dataset = Seq(NGramTestData(
      Array("a", "b", "c", "d", "e"),
      Array("a b c d", "b c d e")
    )).toDF()
    testNGram(nGram, dataset)
  }

  test("empty input yields empty output") {
    val nGram = new NGram()
      .setInputCol("inputTokens")
      .setOutputCol("nGrams")
      .setN(4)
    val dataset = Seq(NGramTestData(Array(), Array())).toDF()
    testNGram(nGram, dataset)
  }

  test("input array < n yields empty output") {
    val nGram = new NGram()
      .setInputCol("inputTokens")
      .setOutputCol("nGrams")
      .setN(6)
    val dataset = Seq(NGramTestData(
      Array("a", "b", "c", "d", "e"),
      Array()
    )).toDF()
    testNGram(nGram, dataset)
  }

  test("read/write") {
    val t = new NGram()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setN(3)
    testDefaultReadWrite(t)
  }
}

object NGramSuite extends SparkFunSuite {

  def testNGram(t: NGram, dataset: Dataset[_]): Unit = {
    t.transform(dataset)
      .select("nGrams", "wantedNGrams")
      .collect()
      .foreach { case Row(actualNGrams, wantedNGrams) =>
        assert(actualNGrams === wantedNGrams)
      }
  }
}
