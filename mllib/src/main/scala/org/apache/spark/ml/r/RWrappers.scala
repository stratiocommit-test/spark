/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.r

import org.apache.hadoop.fs.Path
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkException
import org.apache.spark.ml.util.MLReader

/**
 * This is the Scala stub of SparkR read.ml. It will dispatch the call to corresponding
 * model wrapper loading function according the class name extracted from rMetadata of the path.
 */
private[r] object RWrappers extends MLReader[Object] {

  override def load(path: String): Object = {
    implicit val format = DefaultFormats
    val rMetadataPath = new Path(path, "rMetadata").toString
    val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
    val rMetadata = parse(rMetadataStr)
    val className = (rMetadata \ "class").extract[String]
    className match {
      case "org.apache.spark.ml.r.NaiveBayesWrapper" => NaiveBayesWrapper.load(path)
      case "org.apache.spark.ml.r.AFTSurvivalRegressionWrapper" =>
        AFTSurvivalRegressionWrapper.load(path)
      case "org.apache.spark.ml.r.GeneralizedLinearRegressionWrapper" =>
        GeneralizedLinearRegressionWrapper.load(path)
      case "org.apache.spark.ml.r.KMeansWrapper" =>
        KMeansWrapper.load(path)
      case "org.apache.spark.ml.r.MultilayerPerceptronClassifierWrapper" =>
        MultilayerPerceptronClassifierWrapper.load(path)
      case "org.apache.spark.ml.r.LDAWrapper" =>
        LDAWrapper.load(path)
      case "org.apache.spark.ml.r.IsotonicRegressionWrapper" =>
        IsotonicRegressionWrapper.load(path)
      case "org.apache.spark.ml.r.GaussianMixtureWrapper" =>
        GaussianMixtureWrapper.load(path)
      case "org.apache.spark.ml.r.ALSWrapper" =>
        ALSWrapper.load(path)
      case "org.apache.spark.ml.r.LogisticRegressionWrapper" =>
        LogisticRegressionWrapper.load(path)
      case "org.apache.spark.ml.r.RandomForestRegressorWrapper" =>
        RandomForestRegressorWrapper.load(path)
      case "org.apache.spark.ml.r.RandomForestClassifierWrapper" =>
        RandomForestClassifierWrapper.load(path)
      case "org.apache.spark.ml.r.GBTRegressorWrapper" =>
        GBTRegressorWrapper.load(path)
      case "org.apache.spark.ml.r.GBTClassifierWrapper" =>
        GBTClassifierWrapper.load(path)
      case _ =>
        throw new SparkException(s"SparkR read.ml does not support load $className")
    }
  }
}
