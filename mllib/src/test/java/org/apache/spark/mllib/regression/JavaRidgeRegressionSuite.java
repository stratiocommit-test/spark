/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.regression;

import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.util.LinearDataGenerator;

public class JavaRidgeRegressionSuite extends SharedSparkSession {

  private static double predictionError(List<LabeledPoint> validationData,
                                        RidgeRegressionModel model) {
    double errorSum = 0;
    for (LabeledPoint point : validationData) {
      Double prediction = model.predict(point.features());
      errorSum += (prediction - point.label()) * (prediction - point.label());
    }
    return errorSum / validationData.size();
  }

  private static List<LabeledPoint> generateRidgeData(int numPoints, int numFeatures, double std) {
    // Pick weights as random values distributed uniformly in [-0.5, 0.5]
    Random random = new Random(42);
    double[] w = new double[numFeatures];
    for (int i = 0; i < w.length; i++) {
      w[i] = random.nextDouble() - 0.5;
    }
    return LinearDataGenerator.generateLinearInputAsList(0.0, w, numPoints, 42, std);
  }

  @Test
  public void runRidgeRegressionUsingConstructor() {
    int numExamples = 50;
    int numFeatures = 20;
    List<LabeledPoint> data = generateRidgeData(2 * numExamples, numFeatures, 10.0);

    JavaRDD<LabeledPoint> testRDD = jsc.parallelize(data.subList(0, numExamples));
    List<LabeledPoint> validationData = data.subList(numExamples, 2 * numExamples);

    RidgeRegressionWithSGD ridgeSGDImpl = new RidgeRegressionWithSGD();
    ridgeSGDImpl.optimizer()
      .setStepSize(1.0)
      .setRegParam(0.0)
      .setNumIterations(200);
    RidgeRegressionModel model = ridgeSGDImpl.run(testRDD.rdd());
    double unRegularizedErr = predictionError(validationData, model);

    ridgeSGDImpl.optimizer().setRegParam(0.1);
    model = ridgeSGDImpl.run(testRDD.rdd());
    double regularizedErr = predictionError(validationData, model);

    Assert.assertTrue(regularizedErr < unRegularizedErr);
  }

  @Test
  public void runRidgeRegressionUsingStaticMethods() {
    int numExamples = 50;
    int numFeatures = 20;
    List<LabeledPoint> data = generateRidgeData(2 * numExamples, numFeatures, 10.0);

    JavaRDD<LabeledPoint> testRDD = jsc.parallelize(data.subList(0, numExamples));
    List<LabeledPoint> validationData = data.subList(numExamples, 2 * numExamples);

    RidgeRegressionModel model = RidgeRegressionWithSGD.train(testRDD.rdd(), 200, 1.0, 0.0);
    double unRegularizedErr = predictionError(validationData, model);

    model = RidgeRegressionWithSGD.train(testRDD.rdd(), 200, 1.0, 0.1);
    double regularizedErr = predictionError(validationData, model);

    Assert.assertTrue(regularizedErr < unRegularizedErr);
  }
}
