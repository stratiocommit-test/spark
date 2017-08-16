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

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class JavaBisectingKMeansSuite extends SharedSparkSession {

  @Test
  public void twoDimensionalData() {
    JavaRDD<Vector> points = jsc.parallelize(Lists.newArrayList(
      Vectors.dense(4, -1),
      Vectors.dense(4, 1),
      Vectors.sparse(2, new int[]{0}, new double[]{1.0})
    ), 2);

    BisectingKMeans bkm = new BisectingKMeans()
      .setK(4)
      .setMaxIterations(2)
      .setSeed(1L);
    BisectingKMeansModel model = bkm.run(points);
    Assert.assertEquals(3, model.k());
    Assert.assertArrayEquals(new double[]{3.0, 0.0}, model.root().center().toArray(), 1e-12);
    for (ClusteringTreeNode child : model.root().children()) {
      double[] center = child.center().toArray();
      if (center[0] > 2) {
        Assert.assertEquals(2, child.size());
        Assert.assertArrayEquals(new double[]{4.0, 0.0}, center, 1e-12);
      } else {
        Assert.assertEquals(1, child.size());
        Assert.assertArrayEquals(new double[]{1.0, 0.0}, center, 1e-12);
      }
    }
  }
}
