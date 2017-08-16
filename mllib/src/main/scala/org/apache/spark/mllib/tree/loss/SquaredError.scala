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


/**
 * :: DeveloperApi ::
 * Class for squared error loss calculation.
 *
 * The squared (L2) error is defined as:
 *   (y - F(x))**2
 * where y is the label and F(x) is the model prediction for features x.
 */
@Since("1.2.0")
@DeveloperApi
object SquaredError extends Loss {

  /**
   * Method to calculate the gradients for the gradient boosting calculation for least
   * squares error calculation.
   * The gradient with respect to F(x) is: - 2 (y - F(x))
   * @param prediction Predicted label.
   * @param label True label.
   * @return Loss gradient
   */
  @Since("1.2.0")
  override def gradient(prediction: Double, label: Double): Double = {
    - 2.0 * (label - prediction)
  }

  override private[spark] def computeError(prediction: Double, label: Double): Double = {
    val err = label - prediction
    err * err
  }
}
