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

import scala.Tuple3;

import com.google.common.collect.Lists;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
// $example on$
import org.apache.spark.mllib.clustering.PowerIterationClustering;
import org.apache.spark.mllib.clustering.PowerIterationClusteringModel;
// $example off$

/**
 * Java example for graph clustering using power iteration clustering (PIC).
 */
public class JavaPowerIterationClusteringExample {
  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("JavaPowerIterationClusteringExample");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    @SuppressWarnings("unchecked")
    // $example on$
    JavaRDD<Tuple3<Long, Long, Double>> similarities = sc.parallelize(Lists.newArrayList(
      new Tuple3<>(0L, 1L, 0.9),
      new Tuple3<>(1L, 2L, 0.9),
      new Tuple3<>(2L, 3L, 0.9),
      new Tuple3<>(3L, 4L, 0.1),
      new Tuple3<>(4L, 5L, 0.9)));

    PowerIterationClustering pic = new PowerIterationClustering()
      .setK(2)
      .setMaxIterations(10);
    PowerIterationClusteringModel model = pic.run(similarities);

    for (PowerIterationClustering.Assignment a: model.assignments().toJavaRDD().collect()) {
      System.out.println(a.id() + " -> " + a.cluster());
    }
    // $example off$

    sc.stop();
  }
}
