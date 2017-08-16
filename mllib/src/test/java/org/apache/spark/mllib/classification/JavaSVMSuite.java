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

public class JavaSVMSuite extends SharedSparkSession {

  int validatePrediction(List<LabeledPoint> validationData, SVMModel model) {
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
  public void runSVMUsingConstructor() {
    int nPoints = 10000;
    double A = 2.0;
    double[] weights = {-1.5, 1.0};

    JavaRDD<LabeledPoint> testRDD = jsc.parallelize(SVMSuite.generateSVMInputAsList(A,
      weights, nPoints, 42), 2).cache();
    List<LabeledPoint> validationData =
      SVMSuite.generateSVMInputAsList(A, weights, nPoints, 17);

    SVMWithSGD svmSGDImpl = new SVMWithSGD();
    svmSGDImpl.setIntercept(true);
    svmSGDImpl.optimizer().setStepSize(1.0)
      .setRegParam(1.0)
      .setNumIterations(100);
    SVMModel model = svmSGDImpl.run(testRDD.rdd());

    int numAccurate = validatePrediction(validationData, model);
    Assert.assertTrue(numAccurate > nPoints * 4.0 / 5.0);
  }

  @Test
  public void runSVMUsingStaticMethods() {
    int nPoints = 10000;
    double A = 0.0;
    double[] weights = {-1.5, 1.0};

    JavaRDD<LabeledPoint> testRDD = jsc.parallelize(SVMSuite.generateSVMInputAsList(A,
      weights, nPoints, 42), 2).cache();
    List<LabeledPoint> validationData =
      SVMSuite.generateSVMInputAsList(A, weights, nPoints, 17);

    SVMModel model = SVMWithSGD.train(testRDD.rdd(), 100, 1.0, 1.0, 1.0);

    int numAccurate = validatePrediction(validationData, model);
    Assert.assertTrue(numAccurate > nPoints * 4.0 / 5.0);
  }
}
