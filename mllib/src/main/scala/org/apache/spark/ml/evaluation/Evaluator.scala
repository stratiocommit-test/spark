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

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.sql.Dataset

/**
 * :: DeveloperApi ::
 * Abstract class for evaluators that compute metrics from predictions.
 */
@Since("1.5.0")
@DeveloperApi
abstract class Evaluator extends Params {

  /**
   * Evaluates model output and returns a scalar metric.
   * The value of [[isLargerBetter]] specifies whether larger values are better.
   *
   * @param dataset a dataset that contains labels/observations and predictions.
   * @param paramMap parameter map that specifies the input columns and output metrics
   * @return metric
   */
  @Since("2.0.0")
  def evaluate(dataset: Dataset[_], paramMap: ParamMap): Double = {
    this.copy(paramMap).evaluate(dataset)
  }

  /**
   * Evaluates model output and returns a scalar metric.
   * The value of [[isLargerBetter]] specifies whether larger values are better.
   *
   * @param dataset a dataset that contains labels/observations and predictions.
   * @return metric
   */
  @Since("2.0.0")
  def evaluate(dataset: Dataset[_]): Double

  /**
   * Indicates whether the metric returned by `evaluate` should be maximized (true, default)
   * or minimized (false).
   * A given evaluator may support multiple metrics which may be maximized or minimized.
   */
  @Since("1.5.0")
  def isLargerBetter: Boolean = true

  @Since("1.5.0")
  override def copy(extra: ParamMap): Evaluator
}
