/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class PredictorSuite extends SparkFunSuite with MLlibTestSparkContext {

  import PredictorSuite._

  test("should support all NumericType labels and not support other types") {
    val df = spark.createDataFrame(Seq(
      (0, Vectors.dense(0, 2, 3)),
      (1, Vectors.dense(0, 3, 9)),
      (0, Vectors.dense(0, 2, 6))
    )).toDF("label", "features")

    val types =
      Seq(ShortType, LongType, IntegerType, FloatType, ByteType, DoubleType, DecimalType(10, 0))

    val predictor = new MockPredictor()

    types.foreach { t =>
      predictor.fit(df.select(col("label").cast(t), col("features")))
    }

    intercept[IllegalArgumentException] {
      predictor.fit(df.select(col("label").cast(StringType), col("features")))
    }
  }
}

object PredictorSuite {

  class MockPredictor(override val uid: String)
    extends Predictor[Vector, MockPredictor, MockPredictionModel] {

    def this() = this(Identifiable.randomUID("mockpredictor"))

    override def train(dataset: Dataset[_]): MockPredictionModel = {
      require(dataset.schema("label").dataType == DoubleType)
      new MockPredictionModel(uid)
    }

    override def copy(extra: ParamMap): MockPredictor =
      throw new NotImplementedError()
  }

  class MockPredictionModel(override val uid: String)
    extends PredictionModel[Vector, MockPredictionModel] {

    def this() = this(Identifiable.randomUID("mockpredictormodel"))

    override def predict(features: Vector): Double =
      throw new NotImplementedError()

    override def copy(extra: ParamMap): MockPredictionModel =
      throw new NotImplementedError()
  }
}
