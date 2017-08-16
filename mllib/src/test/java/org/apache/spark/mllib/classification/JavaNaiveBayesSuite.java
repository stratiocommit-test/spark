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

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;


public class JavaNaiveBayesSuite extends SharedSparkSession {

  private static final List<LabeledPoint> POINTS = Arrays.asList(
    new LabeledPoint(0, Vectors.dense(1.0, 0.0, 0.0)),
    new LabeledPoint(0, Vectors.dense(2.0, 0.0, 0.0)),
    new LabeledPoint(1, Vectors.dense(0.0, 1.0, 0.0)),
    new LabeledPoint(1, Vectors.dense(0.0, 2.0, 0.0)),
    new LabeledPoint(2, Vectors.dense(0.0, 0.0, 1.0)),
    new LabeledPoint(2, Vectors.dense(0.0, 0.0, 2.0))
  );

  private int validatePrediction(List<LabeledPoint> points, NaiveBayesModel model) {
    int correct = 0;
    for (LabeledPoint p : points) {
      if (model.predict(p.features()) == p.label()) {
        correct += 1;
      }
    }
    return correct;
  }

  @Test
  public void runUsingConstructor() {
    JavaRDD<LabeledPoint> testRDD = jsc.parallelize(POINTS, 2).cache();

    NaiveBayes nb = new NaiveBayes().setLambda(1.0);
    NaiveBayesModel model = nb.run(testRDD.rdd());

    int numAccurate = validatePrediction(POINTS, model);
    Assert.assertEquals(POINTS.size(), numAccurate);
  }

  @Test
  public void runUsingStaticMethods() {
    JavaRDD<LabeledPoint> testRDD = jsc.parallelize(POINTS, 2).cache();

    NaiveBayesModel model1 = NaiveBayes.train(testRDD.rdd());
    int numAccurate1 = validatePrediction(POINTS, model1);
    Assert.assertEquals(POINTS.size(), numAccurate1);

    NaiveBayesModel model2 = NaiveBayes.train(testRDD.rdd(), 0.5);
    int numAccurate2 = validatePrediction(POINTS, model2);
    Assert.assertEquals(POINTS.size(), numAccurate2);
  }

  @Test
  public void testPredictJavaRDD() {
    JavaRDD<LabeledPoint> examples = jsc.parallelize(POINTS, 2).cache();
    NaiveBayesModel model = NaiveBayes.train(examples.rdd());
    JavaRDD<Vector> vectors = examples.map(new Function<LabeledPoint, Vector>() {
      @Override
      public Vector call(LabeledPoint v) throws Exception {
        return v.features();
      }
    });
    JavaRDD<Double> predictions = model.predict(vectors);
    // Should be able to get the first prediction.
    predictions.first();
  }

  @Test
  public void testModelTypeSetters() {
    NaiveBayes nb = new NaiveBayes()
      .setModelType("bernoulli")
      .setModelType("multinomial");
  }
}
