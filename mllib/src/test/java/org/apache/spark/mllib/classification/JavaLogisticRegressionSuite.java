/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.classification;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;

public class JavaLogisticRegressionSuite extends SharedSparkSession {

  int validatePrediction(List<LabeledPoint> validationData, LogisticRegressionModel model) {
    int numAccurate = 0;
    for (LabeledPoint point : validationData) {
      Double prediction = model.predict(point.features());
      if (prediction == point.label()) {
        numAccurate++;
      }
    }
    return numAccurate;
  }

  @Test
  public void runLRUsingConstructor() {
    int nPoints = 10000;
    double A = 2.0;
    double B = -1.5;

    JavaRDD<LabeledPoint> testRDD = jsc.parallelize(
      LogisticRegressionSuite.generateLogisticInputAsList(A, B, nPoints, 42), 2).cache();
    List<LabeledPoint> validationData =
      LogisticRegressionSuite.generateLogisticInputAsList(A, B, nPoints, 17);

    LogisticRegressionWithSGD lrImpl = new LogisticRegressionWithSGD();
    lrImpl.setIntercept(true);
    lrImpl.optimizer().setStepSize(1.0)
      .setRegParam(1.0)
      .setNumIterations(100);
    LogisticRegressionModel model = lrImpl.run(testRDD.rdd());

    int numAccurate = validatePrediction(validationData, model);
    Assert.assertTrue(numAccurate > nPoints * 4.0 / 5.0);
  }

  @Test
  public void runLRUsingStaticMethods() {
    int nPoints = 10000;
    double A = 0.0;
    double B = -2.5;

    JavaRDD<LabeledPoint> testRDD = jsc.parallelize(
      LogisticRegressionSuite.generateLogisticInputAsList(A, B, nPoints, 42), 2).cache();
    List<LabeledPoint> validationData =
      LogisticRegressionSuite.generateLogisticInputAsList(A, B, nPoints, 17);

    LogisticRegressionModel model = LogisticRegressionWithSGD.train(
      testRDD.rdd(), 100, 1.0, 1.0);

    int numAccurate = validatePrediction(validationData, model);
    Assert.assertTrue(numAccurate > nPoints * 4.0 / 5.0);
  }
}
