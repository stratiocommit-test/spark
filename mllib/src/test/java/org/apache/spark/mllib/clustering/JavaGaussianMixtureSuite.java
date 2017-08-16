/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.clustering;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class JavaGaussianMixtureSuite extends SharedSparkSession {

  @Test
  public void runGaussianMixture() {
    List<Vector> points = Arrays.asList(
      Vectors.dense(1.0, 2.0, 6.0),
      Vectors.dense(1.0, 3.0, 0.0),
      Vectors.dense(1.0, 4.0, 6.0)
    );

    JavaRDD<Vector> data = jsc.parallelize(points, 2);
    GaussianMixtureModel model = new GaussianMixture().setK(2).setMaxIterations(1).setSeed(1234)
      .run(data);
    assertEquals(model.gaussians().length, 2);
    JavaRDD<Integer> predictions = model.predict(data);
    predictions.first();
  }
}
