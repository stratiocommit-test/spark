/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.recommendation;

import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;
import scala.Tuple3;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

public class JavaALSSuite extends SharedSparkSession {

  private void validatePrediction(
    MatrixFactorizationModel model,
    int users,
    int products,
    double[] trueRatings,
    double matchThreshold,
    boolean implicitPrefs,
    double[] truePrefs) {
    List<Tuple2<Integer, Integer>> localUsersProducts = new ArrayList<>(users * products);
    for (int u = 0; u < users; ++u) {
      for (int p = 0; p < products; ++p) {
        localUsersProducts.add(new Tuple2<>(u, p));
      }
    }
    JavaPairRDD<Integer, Integer> usersProducts = jsc.parallelizePairs(localUsersProducts);
    List<Rating> predictedRatings = model.predict(usersProducts).collect();
    Assert.assertEquals(users * products, predictedRatings.size());
    if (!implicitPrefs) {
      for (Rating r : predictedRatings) {
        double prediction = r.rating();
        double correct = trueRatings[r.product() * users + r.user()];
        Assert.assertTrue(String.format("Prediction=%2.4f not below match threshold of %2.2f",
          prediction, matchThreshold), Math.abs(prediction - correct) < matchThreshold);
      }
    } else {
      // For implicit prefs we use the confidence-weighted RMSE to test
      // (ref Mahout's implicit ALS tests)
      double sqErr = 0.0;
      double denom = 0.0;
      for (Rating r : predictedRatings) {
        double prediction = r.rating();
        double truePref = truePrefs[r.product() * users + r.user()];
        double confidence = 1.0 +
          /* alpha = 1.0 * ... */ Math.abs(trueRatings[r.product() * users + r.user()]);
        double err = confidence * (truePref - prediction) * (truePref - prediction);
        sqErr += err;
        denom += confidence;
      }
      double rmse = Math.sqrt(sqErr / denom);
      Assert.assertTrue(String.format("Confidence-weighted RMSE=%2.4f above threshold of %2.2f",
        rmse, matchThreshold), rmse < matchThreshold);
    }
  }

  @Test
  public void runALSUsingStaticMethods() {
    int features = 1;
    int iterations = 15;
    int users = 50;
    int products = 100;
    Tuple3<List<Rating>, double[], double[]> testData =
      ALSSuite.generateRatingsAsJava(users, products, features, 0.7, false, false);

    JavaRDD<Rating> data = jsc.parallelize(testData._1());
    MatrixFactorizationModel model = ALS.train(data.rdd(), features, iterations);
    validatePrediction(model, users, products, testData._2(), 0.3, false, testData._3());
  }

  @Test
  public void runALSUsingConstructor() {
    int features = 2;
    int iterations = 15;
    int users = 100;
    int products = 200;
    Tuple3<List<Rating>, double[], double[]> testData =
      ALSSuite.generateRatingsAsJava(users, products, features, 0.7, false, false);

    JavaRDD<Rating> data = jsc.parallelize(testData._1());

    MatrixFactorizationModel model = new ALS().setRank(features)
      .setIterations(iterations)
      .run(data);
    validatePrediction(model, users, products, testData._2(), 0.3, false, testData._3());
  }

  @Test
  public void runImplicitALSUsingStaticMethods() {
    int features = 1;
    int iterations = 15;
    int users = 80;
    int products = 160;
    Tuple3<List<Rating>, double[], double[]> testData =
      ALSSuite.generateRatingsAsJava(users, products, features, 0.7, true, false);

    JavaRDD<Rating> data = jsc.parallelize(testData._1());
    MatrixFactorizationModel model = ALS.trainImplicit(data.rdd(), features, iterations);
    validatePrediction(model, users, products, testData._2(), 0.4, true, testData._3());
  }

  @Test
  public void runImplicitALSUsingConstructor() {
    int features = 2;
    int iterations = 15;
    int users = 100;
    int products = 200;
    Tuple3<List<Rating>, double[], double[]> testData =
      ALSSuite.generateRatingsAsJava(users, products, features, 0.7, true, false);

    JavaRDD<Rating> data = jsc.parallelize(testData._1());

    MatrixFactorizationModel model = new ALS().setRank(features)
      .setIterations(iterations)
      .setImplicitPrefs(true)
      .run(data.rdd());
    validatePrediction(model, users, products, testData._2(), 0.4, true, testData._3());
  }

  @Test
  public void runImplicitALSWithNegativeWeight() {
    int features = 2;
    int iterations = 15;
    int users = 80;
    int products = 160;
    Tuple3<List<Rating>, double[], double[]> testData =
      ALSSuite.generateRatingsAsJava(users, products, features, 0.7, true, true);

    JavaRDD<Rating> data = jsc.parallelize(testData._1());
    MatrixFactorizationModel model = new ALS().setRank(features)
      .setIterations(iterations)
      .setImplicitPrefs(true)
      .setSeed(8675309L)
      .run(data.rdd());
    validatePrediction(model, users, products, testData._2(), 0.4, true, testData._3());
  }

  @Test
  public void runRecommend() {
    int features = 5;
    int iterations = 10;
    int users = 200;
    int products = 50;
    List<Rating> testData = ALSSuite.generateRatingsAsJava(
      users, products, features, 0.7, true, false)._1();
    JavaRDD<Rating> data = jsc.parallelize(testData);
    MatrixFactorizationModel model = new ALS().setRank(features)
      .setIterations(iterations)
      .setImplicitPrefs(true)
      .setSeed(8675309L)
      .run(data.rdd());
    validateRecommendations(model.recommendProducts(1, 10), 10);
    validateRecommendations(model.recommendUsers(1, 20), 20);
  }

  private static void validateRecommendations(Rating[] recommendations, int howMany) {
    Assert.assertEquals(howMany, recommendations.length);
    for (int i = 1; i < recommendations.length; i++) {
      Assert.assertTrue(recommendations[i - 1].rating() >= recommendations[i].rating());
    }
    Assert.assertTrue(recommendations[0].rating() > 0.7);
  }

}
