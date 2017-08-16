/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.pmml.export

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.classification.{LogisticRegressionModel, SVMModel}
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LassoModel, LinearRegressionModel, RidgeRegressionModel}
import org.apache.spark.mllib.util.LinearDataGenerator

class PMMLModelExportFactorySuite extends SparkFunSuite {

  test("PMMLModelExportFactory create KMeansPMMLModelExport when passing a KMeansModel") {
    val clusterCenters = Array(
      Vectors.dense(1.0, 2.0, 6.0),
      Vectors.dense(1.0, 3.0, 0.0),
      Vectors.dense(1.0, 4.0, 6.0))
    val kmeansModel = new KMeansModel(clusterCenters)

    val modelExport = PMMLModelExportFactory.createPMMLModelExport(kmeansModel)

    assert(modelExport.isInstanceOf[KMeansPMMLModelExport])
  }

  test("PMMLModelExportFactory create GeneralizedLinearPMMLModelExport when passing a "
    + "LinearRegressionModel, RidgeRegressionModel or LassoModel") {
    val linearInput = LinearDataGenerator.generateLinearInput(3.0, Array(10.0, 10.0), 1, 17)

    val linearRegressionModel =
      new LinearRegressionModel(linearInput(0).features, linearInput(0).label)
    val linearModelExport = PMMLModelExportFactory.createPMMLModelExport(linearRegressionModel)
    assert(linearModelExport.isInstanceOf[GeneralizedLinearPMMLModelExport])

    val ridgeRegressionModel =
      new RidgeRegressionModel(linearInput(0).features, linearInput(0).label)
    val ridgeModelExport = PMMLModelExportFactory.createPMMLModelExport(ridgeRegressionModel)
    assert(ridgeModelExport.isInstanceOf[GeneralizedLinearPMMLModelExport])

    val lassoModel = new LassoModel(linearInput(0).features, linearInput(0).label)
    val lassoModelExport = PMMLModelExportFactory.createPMMLModelExport(lassoModel)
    assert(lassoModelExport.isInstanceOf[GeneralizedLinearPMMLModelExport])
  }

  test("PMMLModelExportFactory create BinaryClassificationPMMLModelExport "
    + "when passing a LogisticRegressionModel or SVMModel") {
    val linearInput = LinearDataGenerator.generateLinearInput(3.0, Array(10.0, 10.0), 1, 17)

    val logisticRegressionModel =
      new LogisticRegressionModel(linearInput(0).features, linearInput(0).label)
    val logisticRegressionModelExport =
      PMMLModelExportFactory.createPMMLModelExport(logisticRegressionModel)
    assert(logisticRegressionModelExport.isInstanceOf[BinaryClassificationPMMLModelExport])

    val svmModel = new SVMModel(linearInput(0).features, linearInput(0).label)
    val svmModelExport = PMMLModelExportFactory.createPMMLModelExport(svmModel)
    assert(svmModelExport.isInstanceOf[BinaryClassificationPMMLModelExport])
  }

  test("PMMLModelExportFactory throw IllegalArgumentException "
    + "when passing a Multinomial Logistic Regression") {
    /** 3 classes, 2 features */
    val multiclassLogisticRegressionModel = new LogisticRegressionModel(
      weights = Vectors.dense(0.1, 0.2, 0.3, 0.4), intercept = 1.0,
      numFeatures = 2, numClasses = 3)

    intercept[IllegalArgumentException] {
      PMMLModelExportFactory.createPMMLModelExport(multiclassLogisticRegressionModel)
    }
  }

  test("PMMLModelExportFactory throw IllegalArgumentException when passing an unsupported model") {
    val invalidModel = new Object

    intercept[IllegalArgumentException] {
      PMMLModelExportFactory.createPMMLModelExport(invalidModel)
    }
  }
}
