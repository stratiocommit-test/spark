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

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.util.LinearDataGenerator;

public class JavaLinearRegressionSuite extends SharedSparkSession {

  int validatePrediction(List<LabeledPoint> validationData, LinearRegressionModel model) {
    int numAccurate = 0;
    for (LabeledPoint point : validationData) {
      Double prediction = model.predict(point.features());
      // A prediction is off if the prediction is more than 0.5 away from expected value.
      if (Math.abs(prediction - point.label()) <= 0.5) {
        numAccurate++;
      }
    }
    return numAccurate;
  }

  @Test
  public void runLinearRegressionUsingConstructor() {
    int nPoints = 100;
    double A = 3.0;
    double[] weights = {10, 10};

    JavaRDD<LabeledPoint> testRDD = jsc.parallelize(
      LinearDataGenerator.generateLinearInputAsList(A, weights, nPoints, 42, 0.1), 2).cache();
    List<LabeledPoint> validationData =
      LinearDataGenerator.generateLinearInputAsList(A, weights, nPoints, 17, 0.1);

    LinearRegressionWithSGD linSGDImpl = new LinearRegressionWithSGD();
    linSGDImpl.setIntercept(true);
    LinearRegressionModel model = linSGDImpl.run(testRDD.rdd());

    int numAccurate = validatePrediction(validationData, model);
    Assert.assertTrue(numAccurate > nPoints * 4.0 / 5.0);
  }

  @Test
  public void runLinearRegressionUsingStaticMethods() {
    int nPoints = 100;
    double A = 0.0;
    double[] weights = {10, 10};

    JavaRDD<LabeledPoint> testRDD = jsc.parallelize(
      LinearDataGenerator.generateLinearInputAsList(A, weights, nPoints, 42, 0.1), 2).cache();
    List<LabeledPoint> validationData =
      LinearDataGenerator.generateLinearInputAsList(A, weights, nPoints, 17, 0.1);

    LinearRegressionModel model = LinearRegressionWithSGD.train(testRDD.rdd(), 100);

    int numAccurate = validatePrediction(validationData, model);
    Assert.assertTrue(numAccurate > nPoints * 4.0 / 5.0);
  }

  @Test
  public void testPredictJavaRDD() {
    int nPoints = 100;
    double A = 0.0;
    double[] weights = {10, 10};
    JavaRDD<LabeledPoint> testRDD = jsc.parallelize(
      LinearDataGenerator.generateLinearInputAsList(A, weights, nPoints, 42, 0.1), 2).cache();
    LinearRegressionWithSGD linSGDImpl = new LinearRegressionWithSGD();
    LinearRegressionModel model = linSGDImpl.run(testRDD.rdd());
    JavaRDD<Vector> vectors = testRDD.map(new Function<LabeledPoint, Vector>() {
      @Override
      public Vector call(LabeledPoint v) throws Exception {
        return v.features();
      }
    });
    JavaRDD<Double> predictions = model.predict(vectors);
    // Should be able to get the first prediction.
    predictions.first();
  }
}
