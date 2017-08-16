/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.clustering

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Dataset

class BisectingKMeansSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  final val k = 5
  @transient var dataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    dataset = KMeansSuite.generateKMeansData(spark, 50, 3, k)
  }

  test("default parameters") {
    val bkm = new BisectingKMeans()

    assert(bkm.getK === 4)
    assert(bkm.getFeaturesCol === "features")
    assert(bkm.getPredictionCol === "prediction")
    assert(bkm.getMaxIter === 20)
    assert(bkm.getMinDivisibleClusterSize === 1.0)
    val model = bkm.setMaxIter(1).fit(dataset)

    // copied model must have the same parent
    MLTestingUtils.checkCopy(model)
    assert(model.hasSummary)
    val copiedModel = model.copy(ParamMap.empty)
    assert(copiedModel.hasSummary)
  }

  test("setter/getter") {
    val bkm = new BisectingKMeans()
      .setK(9)
      .setMinDivisibleClusterSize(2.0)
      .setFeaturesCol("test_feature")
      .setPredictionCol("test_prediction")
      .setMaxIter(33)
      .setSeed(123)

    assert(bkm.getK === 9)
    assert(bkm.getFeaturesCol === "test_feature")
    assert(bkm.getPredictionCol === "test_prediction")
    assert(bkm.getMaxIter === 33)
    assert(bkm.getMinDivisibleClusterSize === 2.0)
    assert(bkm.getSeed === 123)

    intercept[IllegalArgumentException] {
      new BisectingKMeans().setK(1)
    }

    intercept[IllegalArgumentException] {
      new BisectingKMeans().setMinDivisibleClusterSize(0)
    }
  }

  test("fit, transform and summary") {
    val predictionColName = "bisecting_kmeans_prediction"
    val bkm = new BisectingKMeans().setK(k).setPredictionCol(predictionColName).setSeed(1)
    val model = bkm.fit(dataset)
    assert(model.clusterCenters.length === k)

    val transformed = model.transform(dataset)
    val expectedColumns = Array("features", predictionColName)
    expectedColumns.foreach { column =>
      assert(transformed.columns.contains(column))
    }
    val clusters =
      transformed.select(predictionColName).rdd.map(_.getInt(0)).distinct().collect().toSet
    assert(clusters.size === k)
    assert(clusters === Set(0, 1, 2, 3, 4))
    assert(model.computeCost(dataset) < 0.1)
    assert(model.hasParent)

    // Check validity of model summary
    val numRows = dataset.count()
    assert(model.hasSummary)
    val summary: BisectingKMeansSummary = model.summary
    assert(summary.predictionCol === predictionColName)
    assert(summary.featuresCol === "features")
    assert(summary.predictions.count() === numRows)
    for (c <- Array(predictionColName, "features")) {
      assert(summary.predictions.columns.contains(c))
    }
    assert(summary.cluster.columns === Array(predictionColName))
    val clusterSizes = summary.clusterSizes
    assert(clusterSizes.length === k)
    assert(clusterSizes.sum === numRows)
    assert(clusterSizes.forall(_ >= 0))

    model.setSummary(None)
    assert(!model.hasSummary)
  }

  test("read/write") {
    def checkModelData(model: BisectingKMeansModel, model2: BisectingKMeansModel): Unit = {
      assert(model.clusterCenters === model2.clusterCenters)
    }
    val bisectingKMeans = new BisectingKMeans()
    testEstimatorAndModelReadWrite(
      bisectingKMeans, dataset, BisectingKMeansSuite.allParamSettings, checkModelData)
  }
}

object BisectingKMeansSuite {
  val allParamSettings: Map[String, Any] = Map(
    "k" -> 3,
    "maxIter" -> 2,
    "seed" -> -1L,
    "minDivisibleClusterSize" -> 2.0
  )
}
