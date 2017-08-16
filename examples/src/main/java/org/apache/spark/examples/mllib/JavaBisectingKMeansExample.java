/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.examples.mllib;

import java.util.ArrayList;

// $example on$
import com.google.common.collect.Lists;
// $example off$
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
// $example on$
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.BisectingKMeans;
import org.apache.spark.mllib.clustering.BisectingKMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
// $example off$

/**
 * Java example for bisecting k-means clustering.
 */
public class JavaBisectingKMeansExample {
  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("JavaBisectingKMeansExample");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    // $example on$
    ArrayList<Vector> localData = Lists.newArrayList(
      Vectors.dense(0.1, 0.1),   Vectors.dense(0.3, 0.3),
      Vectors.dense(10.1, 10.1), Vectors.dense(10.3, 10.3),
      Vectors.dense(20.1, 20.1), Vectors.dense(20.3, 20.3),
      Vectors.dense(30.1, 30.1), Vectors.dense(30.3, 30.3)
    );
    JavaRDD<Vector> data = sc.parallelize(localData, 2);

    BisectingKMeans bkm = new BisectingKMeans()
      .setK(4);
    BisectingKMeansModel model = bkm.run(data);

    System.out.println("Compute Cost: " + model.computeCost(data));

    Vector[] clusterCenters = model.clusterCenters();
    for (int i = 0; i < clusterCenters.length; i++) {
      Vector clusterCenter = clusterCenters[i];
      System.out.println("Cluster Center " + i + ": " + clusterCenter);
    }
    // $example off$

    sc.stop();
  }
}
