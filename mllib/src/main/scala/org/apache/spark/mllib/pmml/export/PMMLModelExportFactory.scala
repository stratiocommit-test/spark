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

import org.dmg.pmml.RegressionNormalizationMethodType

import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.regression.LassoModel
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.RidgeRegressionModel

private[mllib] object PMMLModelExportFactory {

  /**
   * Factory object to help creating the necessary PMMLModelExport implementation
   * taking as input the machine learning model (for example KMeansModel).
   */
  def createPMMLModelExport(model: Any): PMMLModelExport = {
    model match {
      case kmeans: KMeansModel =>
        new KMeansPMMLModelExport(kmeans)
      case linear: LinearRegressionModel =>
        new GeneralizedLinearPMMLModelExport(linear, "linear regression")
      case ridge: RidgeRegressionModel =>
        new GeneralizedLinearPMMLModelExport(ridge, "ridge regression")
      case lasso: LassoModel =>
        new GeneralizedLinearPMMLModelExport(lasso, "lasso regression")
      case svm: SVMModel =>
        new BinaryClassificationPMMLModelExport(
          svm, "linear SVM", RegressionNormalizationMethodType.NONE,
          svm.getThreshold.getOrElse(0.0))
      case logistic: LogisticRegressionModel =>
        if (logistic.numClasses == 2) {
          new BinaryClassificationPMMLModelExport(
            logistic, "logistic regression", RegressionNormalizationMethodType.LOGIT,
            logistic.getThreshold.getOrElse(0.5))
        } else {
          throw new IllegalArgumentException(
            "PMML Export not supported for Multinomial Logistic Regression")
        }
      case _ =>
        throw new IllegalArgumentException(
          "PMML Export not supported for model: " + model.getClass.getName)
    }
  }

}
