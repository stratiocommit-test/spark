/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.regression

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.{PredictionModel, Predictor, PredictorParams}


/**
 * Single-label regression
 *
 * @tparam FeaturesType  Type of input features.  E.g., [[org.apache.spark.mllib.linalg.Vector]]
 * @tparam Learner  Concrete Estimator type
 * @tparam M  Concrete Model type
 */
private[spark] abstract class Regressor[
    FeaturesType,
    Learner <: Regressor[FeaturesType, Learner, M],
    M <: RegressionModel[FeaturesType, M]]
  extends Predictor[FeaturesType, Learner, M] with PredictorParams {

  // TODO: defaultEvaluator (follow-up PR)
}

/**
 * :: DeveloperApi ::
 *
 * Model produced by a [[Regressor]].
 *
 * @tparam FeaturesType  Type of input features.  E.g., [[org.apache.spark.mllib.linalg.Vector]]
 * @tparam M  Concrete Model type.
 */
@DeveloperApi
abstract class RegressionModel[FeaturesType, M <: RegressionModel[FeaturesType, M]]
  extends PredictionModel[FeaturesType, M] with PredictorParams {

  // TODO: defaultEvaluator (follow-up PR)
}
