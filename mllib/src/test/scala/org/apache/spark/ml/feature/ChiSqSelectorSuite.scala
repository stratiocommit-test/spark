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
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{Dataset, Row}

class ChiSqSelectorSuite extends SparkFunSuite with MLlibTestSparkContext
  with DefaultReadWriteTest {

  @transient var dataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Toy dataset, including the top feature for a chi-squared test.
    // These data are chosen such that each feature's test has a distinct p-value.
    /*  To verify the results with R, run:
      library(stats)
      x1 <- c(8.0, 0.0, 0.0, 7.0, 8.0)
      x2 <- c(7.0, 9.0, 9.0, 9.0, 7.0)
      x3 <- c(0.0, 6.0, 8.0, 5.0, 3.0)
      y <- c(0.0, 1.0, 1.0, 2.0, 2.0)
      chisq.test(x1,y)
      chisq.test(x2,y)
      chisq.test(x3,y)
     */
    dataset = spark.createDataFrame(Seq(
      (0.0, Vectors.sparse(3, Array((0, 8.0), (1, 7.0))), Vectors.dense(8.0)),
      (1.0, Vectors.sparse(3, Array((1, 9.0), (2, 6.0))), Vectors.dense(0.0)),
      (1.0, Vectors.dense(Array(0.0, 9.0, 8.0)), Vectors.dense(0.0)),
      (2.0, Vectors.dense(Array(7.0, 9.0, 5.0)), Vectors.dense(7.0)),
      (2.0, Vectors.dense(Array(8.0, 7.0, 3.0)), Vectors.dense(8.0))
    )).toDF("label", "features", "topFeature")
  }

  test("params") {
    ParamsSuite.checkParams(new ChiSqSelector)
    val model = new ChiSqSelectorModel("myModel",
      new org.apache.spark.mllib.feature.ChiSqSelectorModel(Array(1, 3, 4)))
    ParamsSuite.checkParams(model)
  }

  test("Test Chi-Square selector: numTopFeatures") {
    val selector = new ChiSqSelector()
      .setOutputCol("filtered").setSelectorType("numTopFeatures").setNumTopFeatures(1)
    ChiSqSelectorSuite.testSelector(selector, dataset)
  }

  test("Test Chi-Square selector: percentile") {
    val selector = new ChiSqSelector()
      .setOutputCol("filtered").setSelectorType("percentile").setPercentile(0.34)
    ChiSqSelectorSuite.testSelector(selector, dataset)
  }

  test("Test Chi-Square selector: fpr") {
    val selector = new ChiSqSelector()
      .setOutputCol("filtered").setSelectorType("fpr").setFpr(0.2)
    ChiSqSelectorSuite.testSelector(selector, dataset)
  }

  test("read/write") {
    def checkModelData(model: ChiSqSelectorModel, model2: ChiSqSelectorModel): Unit = {
      assert(model.selectedFeatures === model2.selectedFeatures)
    }
    val nb = new ChiSqSelector
    testEstimatorAndModelReadWrite(nb, dataset, ChiSqSelectorSuite.allParamSettings, checkModelData)
  }

  test("should support all NumericType labels and not support other types") {
    val css = new ChiSqSelector()
    MLTestingUtils.checkNumericTypes[ChiSqSelectorModel, ChiSqSelector](
      css, spark) { (expected, actual) =>
        assert(expected.selectedFeatures === actual.selectedFeatures)
      }
  }
}

object ChiSqSelectorSuite {

  private def testSelector(selector: ChiSqSelector, dataset: Dataset[_]): Unit = {
    selector.fit(dataset).transform(dataset).select("filtered", "topFeature").collect()
      .foreach { case Row(vec1: Vector, vec2: Vector) =>
        assert(vec1 ~== vec2 absTol 1e-1)
      }
  }

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "selectorType" -> "percentile",
    "numTopFeatures" -> 1,
    "percentile" -> 0.12,
    "outputCol" -> "myOutput"
  )
}
