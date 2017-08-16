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
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util.{MLReadable, MLReader, MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

private[r] class GaussianMixtureWrapper private (
    val pipeline: PipelineModel,
    val dim: Int,
    val isLoaded: Boolean = false) extends MLWritable {

  private val gmm: GaussianMixtureModel = pipeline.stages(1).asInstanceOf[GaussianMixtureModel]

  lazy val k: Int = gmm.getK

  lazy val lambda: Array[Double] = gmm.weights

  lazy val mu: Array[Double] = gmm.gaussians.flatMap(_.mean.toArray)

  lazy val sigma: Array[Double] = gmm.gaussians.flatMap(_.cov.toArray)

  lazy val vectorToArray = udf { probability: Vector => probability.toArray }
  lazy val posterior: DataFrame = gmm.summary.probability
    .withColumn("posterior", vectorToArray(col(gmm.summary.probabilityCol)))
    .drop(gmm.summary.probabilityCol)

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset).drop(gmm.getFeaturesCol)
  }

  override def write: MLWriter = new GaussianMixtureWrapper.GaussianMixtureWrapperWriter(this)

}

private[r] object GaussianMixtureWrapper extends MLReadable[GaussianMixtureWrapper] {

  def fit(
      data: DataFrame,
      formula: String,
      k: Int,
      maxIter: Int,
      tol: Double): GaussianMixtureWrapper = {

    val rFormula = new RFormula()
      .setFormula(formula)
      .setFeaturesCol("features")
    RWrapperUtils.checkDataColumns(rFormula, data)
    val rFormulaModel = rFormula.fit(data)

    // get feature names from output schema
    val schema = rFormulaModel.transform(data).schema
    val featureAttrs = AttributeGroup.fromStructField(schema(rFormulaModel.getFeaturesCol))
      .attributes.get
    val features = featureAttrs.map(_.name.get)
    val dim = features.length

    val gm = new GaussianMixture()
      .setK(k)
      .setMaxIter(maxIter)
      .setTol(tol)
      .setFeaturesCol(rFormula.getFeaturesCol)

    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, gm))
      .fit(data)

    new GaussianMixtureWrapper(pipeline, dim)
  }

  override def read: MLReader[GaussianMixtureWrapper] = new GaussianMixtureWrapperReader

  override def load(path: String): GaussianMixtureWrapper = super.load(path)

  class GaussianMixtureWrapperWriter(instance: GaussianMixtureWrapper) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadata = ("class" -> instance.getClass.getName) ~
        ("dim" -> instance.dim)
      val rMetadataJson: String = compact(render(rMetadata))

      sc.parallelize(Seq(rMetadataJson), 1).saveAsTextFile(rMetadataPath)
      instance.pipeline.save(pipelinePath)
    }
  }

  class GaussianMixtureWrapperReader extends MLReader[GaussianMixtureWrapper] {

    override def load(path: String): GaussianMixtureWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString
      val pipeline = PipelineModel.load(pipelinePath)

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val dim = (rMetadata \ "dim").extract[Int]
      new GaussianMixtureWrapper(pipeline, dim, isLoaded = true)
    }
  }
}
