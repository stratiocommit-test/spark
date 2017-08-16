/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.feature

import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}

/**
 * Normalizes samples individually to unit L^p^ norm
 *
 * For any 1 &lt;= p &lt; Double.PositiveInfinity, normalizes samples using
 * sum(abs(vector).^p^)^(1/p)^ as norm.
 *
 * For p = Double.PositiveInfinity, max(abs(vector)) will be used as norm for normalization.
 *
 * @param p Normalization in L^p^ space, p = 2 by default.
 */
@Since("1.1.0")
class Normalizer @Since("1.1.0") (p: Double) extends VectorTransformer {

  @Since("1.1.0")
  def this() = this(2)

  require(p >= 1.0)

  /**
   * Applies unit length normalization on a vector.
   *
   * @param vector vector to be normalized.
   * @return normalized vector. If the norm of the input is zero, it will return the input vector.
   */
  @Since("1.1.0")
  override def transform(vector: Vector): Vector = {
    val norm = Vectors.norm(vector, p)

    if (norm != 0.0) {
      // For dense vector, we've to allocate new memory for new output vector.
      // However, for sparse vector, the `index` array will not be changed,
      // so we can re-use it to save memory.
      vector match {
        case DenseVector(vs) =>
          val values = vs.clone()
          val size = values.length
          var i = 0
          while (i < size) {
            values(i) /= norm
            i += 1
          }
          Vectors.dense(values)
        case SparseVector(size, ids, vs) =>
          val values = vs.clone()
          val nnz = values.length
          var i = 0
          while (i < nnz) {
            values(i) /= norm
            i += 1
          }
          Vectors.sparse(size, ids, values)
        case v => throw new IllegalArgumentException("Do not support vector type " + v.getClass)
      }
    } else {
      // Since the norm is zero, return the input vector object itself.
      // Note that it's safe since we always assume that the data in RDD
      // should be immutable.
      vector
    }
  }

}
