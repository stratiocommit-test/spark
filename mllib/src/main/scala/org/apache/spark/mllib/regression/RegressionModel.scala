/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.regression

import org.json4s.{DefaultFormats, JValue}

import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

@Since("0.8.0")
trait RegressionModel extends Serializable {
  /**
   * Predict values for the given data set using the model trained.
   *
   * @param testData RDD representing data points to be predicted
   * @return RDD[Double] where each entry contains the corresponding prediction
   *
   */
  @Since("1.0.0")
  def predict(testData: RDD[Vector]): RDD[Double]

  /**
   * Predict values for a single data point using the model trained.
   *
   * @param testData array representing a single data point
   * @return Double prediction from the trained model
   *
   */
  @Since("1.0.0")
  def predict(testData: Vector): Double

  /**
   * Predict values for examples stored in a JavaRDD.
   * @param testData JavaRDD representing data points to be predicted
   * @return a JavaRDD[java.lang.Double] where each entry contains the corresponding prediction
   *
   */
  @Since("1.0.0")
  def predict(testData: JavaRDD[Vector]): JavaRDD[java.lang.Double] =
    predict(testData.rdd).toJavaRDD().asInstanceOf[JavaRDD[java.lang.Double]]
}

private[mllib] object RegressionModel {

  /**
   * Helper method for loading GLM regression model metadata.
   * @return numFeatures
   */
  def getNumFeatures(metadata: JValue): Int = {
    implicit val formats = DefaultFormats
    (metadata \ "numFeatures").extract[Int]
  }
}
