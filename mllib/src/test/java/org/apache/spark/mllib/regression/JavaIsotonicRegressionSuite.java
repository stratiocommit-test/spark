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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import scala.Tuple3;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;

public class JavaIsotonicRegressionSuite extends SharedSparkSession {

  private static List<Tuple3<Double, Double, Double>> generateIsotonicInput(double[] labels) {
    List<Tuple3<Double, Double, Double>> input = new ArrayList<>(labels.length);

    for (int i = 1; i <= labels.length; i++) {
      input.add(new Tuple3<>(labels[i - 1], (double) i, 1.0));
    }

    return input;
  }

  private IsotonicRegressionModel runIsotonicRegression(double[] labels) {
    JavaRDD<Tuple3<Double, Double, Double>> trainRDD =
      jsc.parallelize(generateIsotonicInput(labels), 2).cache();

    return new IsotonicRegression().run(trainRDD);
  }

  @Test
  public void testIsotonicRegressionJavaRDD() {
    IsotonicRegressionModel model =
      runIsotonicRegression(new double[]{1, 2, 3, 3, 1, 6, 7, 8, 11, 9, 10, 12});

    Assert.assertArrayEquals(
      new double[]{1, 2, 7.0 / 3, 7.0 / 3, 6, 7, 8, 10, 10, 12}, model.predictions(), 1.0e-14);
  }

  @Test
  public void testIsotonicRegressionPredictionsJavaRDD() {
    IsotonicRegressionModel model =
      runIsotonicRegression(new double[]{1, 2, 3, 3, 1, 6, 7, 8, 11, 9, 10, 12});

    JavaDoubleRDD testRDD = jsc.parallelizeDoubles(Arrays.asList(0.0, 1.0, 9.5, 12.0, 13.0));
    List<Double> predictions = model.predict(testRDD).collect();

    Assert.assertEquals(1.0, predictions.get(0).doubleValue(), 1.0e-14);
    Assert.assertEquals(1.0, predictions.get(1).doubleValue(), 1.0e-14);
    Assert.assertEquals(10.0, predictions.get(2).doubleValue(), 1.0e-14);
    Assert.assertEquals(12.0, predictions.get(3).doubleValue(), 1.0e-14);
    Assert.assertEquals(12.0, predictions.get(4).doubleValue(), 1.0e-14);
  }
}
