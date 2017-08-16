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

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

/**
 * A Wrapper of MatrixFactorizationModel to provide helper method for Python.
 */
private[python] class MatrixFactorizationModelWrapper(model: MatrixFactorizationModel)
  extends MatrixFactorizationModel(model.rank, model.userFeatures, model.productFeatures) {

  def predict(userAndProducts: JavaRDD[Array[Any]]): RDD[Rating] =
    predict(SerDe.asTupleRDD(userAndProducts.rdd))

  def getUserFeatures: RDD[Array[Any]] = {
    SerDe.fromTuple2RDD(userFeatures.map {
      case (user, feature) => (user, Vectors.dense(feature))
    }.asInstanceOf[RDD[(Any, Any)]])
  }

  def getProductFeatures: RDD[Array[Any]] = {
    SerDe.fromTuple2RDD(productFeatures.map {
      case (product, feature) => (product, Vectors.dense(feature))
    }.asInstanceOf[RDD[(Any, Any)]])
  }

  def wrappedRecommendProductsForUsers(num: Int): RDD[Array[Any]] = {
    SerDe.fromTuple2RDD(recommendProductsForUsers(num).asInstanceOf[RDD[(Any, Any)]])
  }

  def wrappedRecommendUsersForProducts(num: Int): RDD[Array[Any]] = {
    SerDe.fromTuple2RDD(recommendUsersForProducts(num).asInstanceOf[RDD[(Any, Any)]])
  }
}
