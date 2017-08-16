/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.tree.loss

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.TreeEnsembleModel
import org.apache.spark.rdd.RDD


/**
 * :: DeveloperApi ::
 * Trait for adding "pluggable" loss functions for the gradient boosting algorithm.
 */
@Since("1.2.0")
@DeveloperApi
trait Loss extends Serializable {

  /**
   * Method to calculate the gradients for the gradient boosting calculation.
   * @param prediction Predicted feature
   * @param label true label.
   * @return Loss gradient.
   */
  @Since("1.2.0")
  def gradient(prediction: Double, label: Double): Double

  /**
   * Method to calculate error of the base learner for the gradient boosting calculation.
   *
   * @param model Model of the weak learner.
   * @param data Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   * @return Measure of model error on data
   *
   * @note This method is not used by the gradient boosting algorithm but is useful for debugging
   * purposes.
   */
  @Since("1.2.0")
  def computeError(model: TreeEnsembleModel, data: RDD[LabeledPoint]): Double = {
    data.map(point => computeError(model.predict(point.features), point.label)).mean()
  }

  /**
   * Method to calculate loss when the predictions are already known.
   *
   * @param prediction Predicted label.
   * @param label True label.
   * @return Measure of model error on datapoint.
   *
   * @note This method is used in the method evaluateEachIteration to avoid recomputing the
   * predicted values from previously fit trees.
   */
  private[spark] def computeError(prediction: Double, label: Double): Double
}
