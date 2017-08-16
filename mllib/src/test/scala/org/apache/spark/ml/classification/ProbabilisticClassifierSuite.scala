/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.classification

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.{Vector, Vectors}

final class TestProbabilisticClassificationModel(
    override val uid: String,
    override val numFeatures: Int,
    override val numClasses: Int)
  extends ProbabilisticClassificationModel[Vector, TestProbabilisticClassificationModel] {

  override def copy(extra: org.apache.spark.ml.param.ParamMap): this.type = defaultCopy(extra)

  override protected def predictRaw(input: Vector): Vector = {
    input
  }

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction
  }

  def friendlyPredict(values: Double*): Double = {
    predict(Vectors.dense(values.toArray))
  }
}


class ProbabilisticClassifierSuite extends SparkFunSuite {

  test("test thresholding") {
    val testModel = new TestProbabilisticClassificationModel("myuid", 2, 2)
      .setThresholds(Array(0.5, 0.2))
    assert(testModel.friendlyPredict(1.0, 1.0) === 1.0)
    assert(testModel.friendlyPredict(1.0, 0.2) === 0.0)
  }

  test("test thresholding not required") {
    val testModel = new TestProbabilisticClassificationModel("myuid", 2, 2)
    assert(testModel.friendlyPredict(1.0, 2.0) === 1.0)
  }

  test("test tiebreak") {
    val testModel = new TestProbabilisticClassificationModel("myuid", 2, 2)
      .setThresholds(Array(0.4, 0.4))
    assert(testModel.friendlyPredict(0.6, 0.6) === 0.0)
  }

  test("test one zero threshold") {
    val testModel = new TestProbabilisticClassificationModel("myuid", 2, 2)
      .setThresholds(Array(0.0, 0.1))
    assert(testModel.friendlyPredict(1.0, 10.0) === 0.0)
    assert(testModel.friendlyPredict(0.0, 10.0) === 1.0)
  }

  test("bad thresholds") {
    intercept[IllegalArgumentException] {
      new TestProbabilisticClassificationModel("myuid", 2, 2).setThresholds(Array(0.0, 0.0))
    }
    intercept[IllegalArgumentException] {
      new TestProbabilisticClassificationModel("myuid", 2, 2).setThresholds(Array(-0.1, 0.1))
    }
  }
}

object ProbabilisticClassifierSuite {

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = ClassifierSuite.allParamSettings ++ Map(
    "probabilityCol" -> "myProbability",
    "thresholds" -> Array(0.4, 0.6)
  )

}
