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
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

/**
 * A feature transformer that projects vectors to a low-dimensional space using PCA.
 *
 * @param k number of principal components
 */
@Since("1.4.0")
class PCA @Since("1.4.0") (@Since("1.4.0") val k: Int) {
  require(k > 0,
    s"Number of principal components must be positive but got ${k}")

  /**
   * Computes a [[PCAModel]] that contains the principal components of the input vectors.
   *
   * @param sources source vectors
   */
  @Since("1.4.0")
  def fit(sources: RDD[Vector]): PCAModel = {
    val numFeatures = sources.first().size
    require(k <= numFeatures,
      s"source vector size $numFeatures must be no less than k=$k")

    val mat = new RowMatrix(sources)
    val (pc, explainedVariance) = mat.computePrincipalComponentsAndExplainedVariance(k)
    val densePC = pc match {
      case dm: DenseMatrix =>
        dm
      case sm: SparseMatrix =>
        /* Convert a sparse matrix to dense.
         *
         * RowMatrix.computePrincipalComponents always returns a dense matrix.
         * The following code is a safeguard.
         */
        sm.toDense
      case m =>
        throw new IllegalArgumentException("Unsupported matrix format. Expected " +
          s"SparseMatrix or DenseMatrix. Instead got: ${m.getClass}")
    }
    val denseExplainedVariance = explainedVariance match {
      case dv: DenseVector =>
        dv
      case sv: SparseVector =>
        sv.toDense
    }
    new PCAModel(k, densePC, denseExplainedVariance)
  }

  /**
   * Java-friendly version of `fit()`.
   */
  @Since("1.4.0")
  def fit(sources: JavaRDD[Vector]): PCAModel = fit(sources.rdd)
}

/**
 * Model fitted by [[PCA]] that can project vectors to a low-dimensional space using PCA.
 *
 * @param k number of principal components.
 * @param pc a principal components Matrix. Each column is one principal component.
 */
@Since("1.4.0")
class PCAModel private[spark] (
    @Since("1.4.0") val k: Int,
    @Since("1.4.0") val pc: DenseMatrix,
    @Since("1.6.0") val explainedVariance: DenseVector) extends VectorTransformer {
  /**
   * Transform a vector by computed Principal Components.
   *
   * @param vector vector to be transformed.
   *               Vector must be the same length as the source vectors given to `PCA.fit()`.
   * @return transformed vector. Vector will be of length k.
   */
  @Since("1.4.0")
  override def transform(vector: Vector): Vector = {
    vector match {
      case dv: DenseVector =>
        pc.transpose.multiply(dv)
      case SparseVector(size, indices, values) =>
        /* SparseVector -> single row SparseMatrix */
        val sm = Matrices.sparse(size, 1, Array(0, indices.length), indices, values).transpose
        val projection = sm.multiply(pc)
        Vectors.dense(projection.values)
      case _ =>
        throw new IllegalArgumentException("Unsupported vector format. Expected " +
          s"SparseVector or DenseVector. Instead got: ${vector.getClass}")
    }
  }
}
