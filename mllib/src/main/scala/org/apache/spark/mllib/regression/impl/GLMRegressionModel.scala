/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.regression.impl

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.util.Loader
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Helper methods for import/export of GLM regression models.
 */
private[regression] object GLMRegressionModel {

  object SaveLoadV1_0 {

    def thisFormatVersion: String = "1.0"

    /** Model data for model import/export */
    case class Data(weights: Vector, intercept: Double)

    /**
     * Helper method for saving GLM regression model metadata and data.
     * @param modelClass  String name for model class, to be saved with metadata
     */
    def save(
        sc: SparkContext,
        path: String,
        modelClass: String,
        weights: Vector,
        intercept: Double): Unit = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()

      // Create JSON metadata.
      val metadata = compact(render(
        ("class" -> modelClass) ~ ("version" -> thisFormatVersion) ~
          ("numFeatures" -> weights.size)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      // Create Parquet data.
      val data = Data(weights, intercept)
      spark.createDataFrame(Seq(data)).repartition(1).write.parquet(Loader.dataPath(path))
    }

    /**
     * Helper method for loading GLM regression model data.
     * @param modelClass  String name for model class (used for error messages)
     * @param numFeatures  Number of features, to be checked against loaded data.
     *                     The length of the weights vector should equal numFeatures.
     */
    def loadData(sc: SparkContext, path: String, modelClass: String, numFeatures: Int): Data = {
      val dataPath = Loader.dataPath(path)
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val dataRDD = spark.read.parquet(dataPath)
      val dataArray = dataRDD.select("weights", "intercept").take(1)
      assert(dataArray.length == 1, s"Unable to load $modelClass data from: $dataPath")
      val data = dataArray(0)
      assert(data.size == 2, s"Unable to load $modelClass data from: $dataPath")
      data match {
        case Row(weights: Vector, intercept: Double) =>
          assert(weights.size == numFeatures, s"Expected $numFeatures features, but" +
            s" found ${weights.size} features when loading $modelClass weights from $dataPath")
          Data(weights, intercept)
      }
    }
  }

}
