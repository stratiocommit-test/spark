/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.stat

import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.Vector

/**
 * Trait for multivariate statistical summary of a data matrix.
 */
@Since("1.0.0")
trait MultivariateStatisticalSummary {

  /**
   * Sample mean vector.
   */
  @Since("1.0.0")
  def mean: Vector

  /**
   * Sample variance vector. Should return a zero vector if the sample size is 1.
   */
  @Since("1.0.0")
  def variance: Vector

  /**
   * Sample size.
   */
  @Since("1.0.0")
  def count: Long

  /**
   * Number of nonzero elements (including explicitly presented zero values) in each column.
   */
  @Since("1.0.0")
  def numNonzeros: Vector

  /**
   * Maximum value of each column.
   */
  @Since("1.0.0")
  def max: Vector

  /**
   * Minimum value of each column.
   */
  @Since("1.0.0")
  def min: Vector

  /**
   * Euclidean magnitude of each column
   */
  @Since("1.2.0")
  def normL2: Vector

  /**
   * L1 norm of each column
   */
  @Since("1.2.0")
  def normL1: Vector
}
