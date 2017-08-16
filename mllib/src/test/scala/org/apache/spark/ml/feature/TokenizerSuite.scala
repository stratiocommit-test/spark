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
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{Dataset, Row}

@BeanInfo
case class TokenizerTestData(rawText: String, wantedTokens: Array[String])

class TokenizerSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("params") {
    ParamsSuite.checkParams(new Tokenizer)
  }

  test("read/write") {
    val t = new Tokenizer()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
    testDefaultReadWrite(t)
  }
}

class RegexTokenizerSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import org.apache.spark.ml.feature.RegexTokenizerSuite._
  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new RegexTokenizer)
  }

  test("RegexTokenizer") {
    val tokenizer0 = new RegexTokenizer()
      .setGaps(false)
      .setPattern("\\w+|\\p{Punct}")
      .setInputCol("rawText")
      .setOutputCol("tokens")
    val dataset0 = Seq(
      TokenizerTestData("Test for tokenization.", Array("test", "for", "tokenization", ".")),
      TokenizerTestData("Te,st. punct", Array("te", ",", "st", ".", "punct"))
    ).toDF()
    testRegexTokenizer(tokenizer0, dataset0)

    val dataset1 = Seq(
      TokenizerTestData("Test for tokenization.", Array("test", "for", "tokenization")),
      TokenizerTestData("Te,st. punct", Array("punct"))
    ).toDF()
    tokenizer0.setMinTokenLength(3)
    testRegexTokenizer(tokenizer0, dataset1)

    val tokenizer2 = new RegexTokenizer()
      .setInputCol("rawText")
      .setOutputCol("tokens")
    val dataset2 = Seq(
      TokenizerTestData("Test for tokenization.", Array("test", "for", "tokenization.")),
      TokenizerTestData("Te,st.  punct", Array("te,st.", "punct"))
    ).toDF()
    testRegexTokenizer(tokenizer2, dataset2)
  }

  test("RegexTokenizer with toLowercase false") {
    val tokenizer = new RegexTokenizer()
      .setInputCol("rawText")
      .setOutputCol("tokens")
      .setToLowercase(false)
    val dataset = Seq(
      TokenizerTestData("JAVA SCALA", Array("JAVA", "SCALA")),
      TokenizerTestData("java scala", Array("java", "scala"))
    ).toDF()
    testRegexTokenizer(tokenizer, dataset)
  }

  test("read/write") {
    val t = new RegexTokenizer()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setMinTokenLength(2)
      .setGaps(false)
      .setPattern("hi")
      .setToLowercase(false)
    testDefaultReadWrite(t)
  }
}

object RegexTokenizerSuite extends SparkFunSuite {

  def testRegexTokenizer(t: RegexTokenizer, dataset: Dataset[_]): Unit = {
    t.transform(dataset)
      .select("tokens", "wantedTokens")
      .collect()
      .foreach { case Row(tokens, wantedTokens) =>
        assert(tokens === wantedTokens)
      }
  }
}
