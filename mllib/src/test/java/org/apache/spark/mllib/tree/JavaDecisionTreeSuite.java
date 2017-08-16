/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.tree;

import java.util.HashMap;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.configuration.Algo;
import org.apache.spark.mllib.tree.configuration.Strategy;
import org.apache.spark.mllib.tree.impurity.Gini;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;

public class JavaDecisionTreeSuite extends SharedSparkSession {

  int validatePrediction(List<LabeledPoint> validationData, DecisionTreeModel model) {
    int numCorrect = 0;
    for (LabeledPoint point : validationData) {
      Double prediction = model.predict(point.features());
      if (prediction == point.label()) {
        numCorrect++;
      }
    }
    return numCorrect;
  }

  @Test
  public void runDTUsingConstructor() {
    List<LabeledPoint> arr = DecisionTreeSuite.generateCategoricalDataPointsAsJavaList();
    JavaRDD<LabeledPoint> rdd = jsc.parallelize(arr);
    HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
    categoricalFeaturesInfo.put(1, 2); // feature 1 has 2 categories

    int maxDepth = 4;
    int numClasses = 2;
    int maxBins = 100;
    Strategy strategy = new Strategy(Algo.Classification(), Gini.instance(), maxDepth, numClasses,
      maxBins, categoricalFeaturesInfo);

    DecisionTree learner = new DecisionTree(strategy);
    DecisionTreeModel model = learner.run(rdd.rdd());

    int numCorrect = validatePrediction(arr, model);
    Assert.assertTrue(numCorrect == rdd.count());
  }

  @Test
  public void runDTUsingStaticMethods() {
    List<LabeledPoint> arr = DecisionTreeSuite.generateCategoricalDataPointsAsJavaList();
    JavaRDD<LabeledPoint> rdd = jsc.parallelize(arr);
    HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
    categoricalFeaturesInfo.put(1, 2); // feature 1 has 2 categories

    int maxDepth = 4;
    int numClasses = 2;
    int maxBins = 100;
    Strategy strategy = new Strategy(Algo.Classification(), Gini.instance(), maxDepth, numClasses,
      maxBins, categoricalFeaturesInfo);

    DecisionTreeModel model = DecisionTree$.MODULE$.train(rdd.rdd(), strategy);

    // java compatibility test
    JavaRDD<Double> predictions = model.predict(rdd.map(new Function<LabeledPoint, Vector>() {
      @Override
      public Vector call(LabeledPoint v1) {
        return v1.features();
      }
    }));

    int numCorrect = validatePrediction(arr, model);
    Assert.assertTrue(numCorrect == rdd.count());
  }

}
