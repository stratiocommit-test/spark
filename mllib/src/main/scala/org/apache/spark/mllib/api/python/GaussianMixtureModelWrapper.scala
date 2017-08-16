/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.api.python

import scala.collection.JavaConverters

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.GaussianMixtureModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
 * Wrapper around GaussianMixtureModel to provide helper methods in Python
 */
private[python] class GaussianMixtureModelWrapper(model: GaussianMixtureModel) {
  val weights: Vector = Vectors.dense(model.weights)
  val k: Int = weights.size

  /**
   * Returns gaussians as a List of Vectors and Matrices corresponding each MultivariateGaussian
   */
  val gaussians: Array[Byte] = {
    val modelGaussians = model.gaussians.map { gaussian =>
      Array[Any](gaussian.mu, gaussian.sigma)
    }
    SerDe.dumps(JavaConverters.seqAsJavaListConverter(modelGaussians).asJava)
  }

  def predictSoft(point: Vector): Vector = {
    Vectors.dense(model.predictSoft(point))
  }

  def save(sc: SparkContext, path: String): Unit = model.save(sc, path)
}
