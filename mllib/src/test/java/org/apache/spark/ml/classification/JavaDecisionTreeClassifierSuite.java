/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.classification;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.tree.impl.TreeTests;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JavaDecisionTreeClassifierSuite extends SharedSparkSession {

  @Test
  public void runDT() {
    int nPoints = 20;
    double A = 2.0;
    double B = -1.5;

    JavaRDD<LabeledPoint> data = jsc.parallelize(
      LogisticRegressionSuite.generateLogisticInputAsList(A, B, nPoints, 42), 2).cache();
    Map<Integer, Integer> categoricalFeatures = new HashMap<>();
    Dataset<Row> dataFrame = TreeTests.setMetadata(data, categoricalFeatures, 2);

    // This tests setters. Training with various options is tested in Scala.
    DecisionTreeClassifier dt = new DecisionTreeClassifier()
      .setMaxDepth(2)
      .setMaxBins(10)
      .setMinInstancesPerNode(5)
      .setMinInfoGain(0.0)
      .setMaxMemoryInMB(256)
      .setCacheNodeIds(false)
      .setCheckpointInterval(10)
      .setMaxDepth(2); // duplicate setMaxDepth to check builder pattern
    for (String impurity : DecisionTreeClassifier.supportedImpurities()) {
      dt.setImpurity(impurity);
    }
    DecisionTreeClassificationModel model = dt.fit(dataFrame);

    model.transform(dataFrame);
    model.numNodes();
    model.depth();
    model.toDebugString();

    /*
    // TODO: Add test once save/load are implemented.  SPARK-6725
    File tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "spark");
    String path = tempDir.toURI().toString();
    try {
      model3.save(sc.sc(), path);
      DecisionTreeClassificationModel sameModel =
        DecisionTreeClassificationModel.load(sc.sc(), path);
      TreeTests.checkEqual(model3, sameModel);
    } finally {
      Utils.deleteRecursively(tempDir);
    }
    */
  }
}
