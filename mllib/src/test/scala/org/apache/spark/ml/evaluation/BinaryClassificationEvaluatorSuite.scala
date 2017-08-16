/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.evaluation

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.util.MLlibTestSparkContext

class BinaryClassificationEvaluatorSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new BinaryClassificationEvaluator)
  }

  test("read/write") {
    val evaluator = new BinaryClassificationEvaluator()
      .setRawPredictionCol("myRawPrediction")
      .setLabelCol("myLabel")
      .setMetricName("areaUnderPR")
    testDefaultReadWrite(evaluator)
  }

  test("should accept both vector and double raw prediction col") {
    val evaluator = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderPR")

    val vectorDF = Seq(
      (0d, Vectors.dense(12, 2.5)),
      (1d, Vectors.dense(1, 3)),
      (0d, Vectors.dense(10, 2))
    ).toDF("label", "rawPrediction")
    assert(evaluator.evaluate(vectorDF) === 1.0)

    val doubleDF = Seq(
      (0d, 0d),
      (1d, 1d),
      (0d, 0d)
    ).toDF("label", "rawPrediction")
    assert(evaluator.evaluate(doubleDF) === 1.0)

    val stringDF = Seq(
      (0d, "0d"),
      (1d, "1d"),
      (0d, "0d")
    ).toDF("label", "rawPrediction")
    val thrown = intercept[IllegalArgumentException] {
      evaluator.evaluate(stringDF)
    }
    assert(thrown.getMessage.replace("\n", "") contains "Column rawPrediction must be of type " +
      "equal to one of the following types: [DoubleType, ")
    assert(thrown.getMessage.replace("\n", "") contains "but was actually of type StringType.")
  }

  test("should support all NumericType labels and not support other types") {
    val evaluator = new BinaryClassificationEvaluator().setRawPredictionCol("prediction")
    MLTestingUtils.checkNumericTypes(evaluator, spark)
  }
}
