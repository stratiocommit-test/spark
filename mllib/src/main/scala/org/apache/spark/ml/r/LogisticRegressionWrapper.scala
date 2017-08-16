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
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{IndexToString, RFormula}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.r.RWrapperUtils._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class LogisticRegressionWrapper private (
    val pipeline: PipelineModel,
    val features: Array[String],
    val labels: Array[String]) extends MLWritable {

  import LogisticRegressionWrapper._

  private val lrModel: LogisticRegressionModel =
    pipeline.stages(1).asInstanceOf[LogisticRegressionModel]

  val rFeatures: Array[String] = if (lrModel.getFitIntercept) {
    Array("(Intercept)") ++ features
  } else {
    features
  }

  val rCoefficients: Array[Double] = {
    val numRows = lrModel.coefficientMatrix.numRows
    val numCols = lrModel.coefficientMatrix.numCols
    val numColsWithIntercept = if (lrModel.getFitIntercept) numCols + 1 else numCols
    val coefficients: Array[Double] = new Array[Double](numRows * numColsWithIntercept)
    val coefficientVectors: Seq[Vector] = lrModel.coefficientMatrix.rowIter.toSeq
    var i = 0
    if (lrModel.getFitIntercept) {
      while (i < numRows) {
        coefficients(i * numColsWithIntercept) = lrModel.interceptVector(i)
        System.arraycopy(coefficientVectors(i).toArray, 0,
          coefficients, i * numColsWithIntercept + 1, numCols)
        i += 1
      }
    } else {
      while (i < numRows) {
        System.arraycopy(coefficientVectors(i).toArray, 0,
          coefficients, i * numColsWithIntercept, numCols)
        i += 1
      }
    }
    coefficients
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset)
      .drop(PREDICTED_LABEL_INDEX_COL)
      .drop(lrModel.getFeaturesCol)
      .drop(lrModel.getLabelCol)
  }

  override def write: MLWriter = new LogisticRegressionWrapper.LogisticRegressionWrapperWriter(this)
}

private[r] object LogisticRegressionWrapper
    extends MLReadable[LogisticRegressionWrapper] {

  val PREDICTED_LABEL_INDEX_COL = "pred_label_idx"
  val PREDICTED_LABEL_COL = "prediction"

  def fit( // scalastyle:ignore
      data: DataFrame,
      formula: String,
      regParam: Double,
      elasticNetParam: Double,
      maxIter: Int,
      tol: Double,
      family: String,
      standardization: Boolean,
      thresholds: Array[Double],
      weightCol: String
      ): LogisticRegressionWrapper = {

    val rFormula = new RFormula()
      .setFormula(formula)
      .setForceIndexLabel(true)
    checkDataColumns(rFormula, data)
    val rFormulaModel = rFormula.fit(data)

    val fitIntercept = rFormula.hasIntercept

    // get labels and feature names from output schema
    val (features, labels) = getFeaturesAndLabels(rFormulaModel, data)

    // assemble and fit the pipeline
    val lr = new LogisticRegression()
      .setRegParam(regParam)
      .setElasticNetParam(elasticNetParam)
      .setMaxIter(maxIter)
      .setTol(tol)
      .setFitIntercept(fitIntercept)
      .setFamily(family)
      .setStandardization(standardization)
      .setWeightCol(weightCol)
      .setFeaturesCol(rFormula.getFeaturesCol)
      .setLabelCol(rFormula.getLabelCol)
      .setPredictionCol(PREDICTED_LABEL_INDEX_COL)

    if (thresholds.length > 1) {
      lr.setThresholds(thresholds)
    } else {
      lr.setThreshold(thresholds(0))
    }

    val idxToStr = new IndexToString()
      .setInputCol(PREDICTED_LABEL_INDEX_COL)
      .setOutputCol(PREDICTED_LABEL_COL)
      .setLabels(labels)

    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, lr, idxToStr))
      .fit(data)

    new LogisticRegressionWrapper(pipeline, features, labels)
  }

  override def read: MLReader[LogisticRegressionWrapper] = new LogisticRegressionWrapperReader

  override def load(path: String): LogisticRegressionWrapper = super.load(path)

  class LogisticRegressionWrapperWriter(instance: LogisticRegressionWrapper) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadata = ("class" -> instance.getClass.getName) ~
        ("features" -> instance.features.toSeq) ~
        ("labels" -> instance.labels.toSeq)
      val rMetadataJson: String = compact(render(rMetadata))
      sc.parallelize(Seq(rMetadataJson), 1).saveAsTextFile(rMetadataPath)

      instance.pipeline.save(pipelinePath)
    }
  }

  class LogisticRegressionWrapperReader extends MLReader[LogisticRegressionWrapper] {

    override def load(path: String): LogisticRegressionWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val features = (rMetadata \ "features").extract[Array[String]]
      val labels = (rMetadata \ "labels").extract[Array[String]]

      val pipeline = PipelineModel.load(pipelinePath)
      new LogisticRegressionWrapper(pipeline, features, labels)
    }
  }
}
