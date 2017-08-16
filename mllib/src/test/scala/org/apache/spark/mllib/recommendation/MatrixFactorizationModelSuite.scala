/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.recommendation

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

class MatrixFactorizationModelSuite extends SparkFunSuite with MLlibTestSparkContext {

  val rank = 2
  var userFeatures: RDD[(Int, Array[Double])] = _
  var prodFeatures: RDD[(Int, Array[Double])] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    userFeatures = sc.parallelize(Seq((0, Array(1.0, 2.0)), (1, Array(3.0, 4.0))))
    prodFeatures = sc.parallelize(Seq((2, Array(5.0, 6.0))))
  }

  test("constructor") {
    val model = new MatrixFactorizationModel(rank, userFeatures, prodFeatures)
    assert(model.predict(0, 2) ~== 17.0 relTol 1e-14)

    intercept[IllegalArgumentException] {
      new MatrixFactorizationModel(1, userFeatures, prodFeatures)
    }

    val userFeatures1 = sc.parallelize(Seq((0, Array(1.0)), (1, Array(3.0))))
    intercept[IllegalArgumentException] {
      new MatrixFactorizationModel(rank, userFeatures1, prodFeatures)
    }

    val prodFeatures1 = sc.parallelize(Seq((2, Array(5.0))))
    intercept[IllegalArgumentException] {
      new MatrixFactorizationModel(rank, userFeatures, prodFeatures1)
    }
  }

  test("save/load") {
    val model = new MatrixFactorizationModel(rank, userFeatures, prodFeatures)
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString
    def collect(features: RDD[(Int, Array[Double])]): Set[(Int, Seq[Double])] = {
      features.mapValues(_.toSeq).collect().toSet
    }
    try {
      model.save(sc, path)
      val newModel = MatrixFactorizationModel.load(sc, path)
      assert(newModel.rank === rank)
      assert(collect(newModel.userFeatures) === collect(userFeatures))
      assert(collect(newModel.productFeatures) === collect(prodFeatures))
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  test("batch predict API recommendProductsForUsers") {
    val model = new MatrixFactorizationModel(rank, userFeatures, prodFeatures)
    val topK = 10
    val recommendations = model.recommendProductsForUsers(topK).collectAsMap()

    assert(recommendations(0)(0).rating ~== 17.0 relTol 1e-14)
    assert(recommendations(1)(0).rating ~== 39.0 relTol 1e-14)
  }

  test("batch predict API recommendUsersForProducts") {
    val model = new MatrixFactorizationModel(rank, userFeatures, prodFeatures)
    val topK = 10
    val recommendations = model.recommendUsersForProducts(topK).collectAsMap()

    assert(recommendations(2)(0).user == 1)
    assert(recommendations(2)(0).rating ~== 39.0 relTol 1e-14)
    assert(recommendations(2)(1).user == 0)
    assert(recommendations(2)(1).rating ~== 17.0 relTol 1e-14)
  }
}
