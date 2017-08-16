/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.stat.correlation

import org.apache.spark.mllib.linalg.{DenseVector, Matrix, Vector}
import org.apache.spark.rdd.RDD

/**
 * Trait for correlation algorithms.
 */
private[stat] trait Correlation {

  /**
   * Compute correlation for two datasets.
   */
  def computeCorrelation(x: RDD[Double], y: RDD[Double]): Double

  /**
   * Compute the correlation matrix S, for the input matrix, where S(i, j) is the correlation
   * between column i and j. S(i, j) can be NaN if the correlation is undefined for column i and j.
   */
  def computeCorrelationMatrix(X: RDD[Vector]): Matrix

  /**
   * Combine the two input RDD[Double]s into an RDD[Vector] and compute the correlation using the
   * correlation implementation for RDD[Vector]. Can be NaN if correlation is undefined for the
   * input vectors.
   */
  def computeCorrelationWithMatrixImpl(x: RDD[Double], y: RDD[Double]): Double = {
    val mat: RDD[Vector] = x.zip(y).map { case (xi, yi) => new DenseVector(Array(xi, yi)) }
    computeCorrelationMatrix(mat)(0, 1)
  }

}

/**
 * Delegates computation to the specific correlation object based on the input method name.
 */
private[stat] object Correlations {

  def corr(x: RDD[Double],
       y: RDD[Double],
       method: String = CorrelationNames.defaultCorrName): Double = {
    val correlation = getCorrelationFromName(method)
    correlation.computeCorrelation(x, y)
  }

  def corrMatrix(X: RDD[Vector],
      method: String = CorrelationNames.defaultCorrName): Matrix = {
    val correlation = getCorrelationFromName(method)
    correlation.computeCorrelationMatrix(X)
  }

  // Match input correlation name with a known name via simple string matching.
  def getCorrelationFromName(method: String): Correlation = {
    try {
      CorrelationNames.nameToObjectMap(method)
    } catch {
      case nse: NoSuchElementException =>
        throw new IllegalArgumentException("Unrecognized method name. Supported correlations: "
          + CorrelationNames.nameToObjectMap.keys.mkString(", "))
    }
  }
}

/**
 * Maintains supported and default correlation names.
 *
 * Currently supported correlations: `pearson`, `spearman`.
 * Current default correlation: `pearson`.
 *
 * After new correlation algorithms are added, please update the documentation here and in
 * Statistics.scala for the correlation APIs.
 */
private[mllib] object CorrelationNames {

  // Note: after new types of correlations are implemented, please update this map.
  val nameToObjectMap = Map(("pearson", PearsonCorrelation), ("spearman", SpearmanCorrelation))
  val defaultCorrName: String = "pearson"

}
