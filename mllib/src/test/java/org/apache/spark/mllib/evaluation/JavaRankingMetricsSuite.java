/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.evaluation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import scala.Tuple2;
import scala.Tuple2$;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;

public class JavaRankingMetricsSuite extends SharedSparkSession {
  private transient JavaRDD<Tuple2<List<Integer>, List<Integer>>> predictionAndLabels;

  @Override
  public void setUp() throws IOException {
    super.setUp();
    predictionAndLabels = jsc.parallelize(Arrays.asList(
      Tuple2$.MODULE$.apply(
        Arrays.asList(1, 6, 2, 7, 8, 3, 9, 10, 4, 5), Arrays.asList(1, 2, 3, 4, 5)),
      Tuple2$.MODULE$.apply(
        Arrays.asList(4, 1, 5, 6, 2, 7, 3, 8, 9, 10), Arrays.asList(1, 2, 3)),
      Tuple2$.MODULE$.apply(
        Arrays.asList(1, 2, 3, 4, 5), Arrays.<Integer>asList())), 2);
  }

  @Test
  public void rankingMetrics() {
    @SuppressWarnings("unchecked")
    RankingMetrics<?> metrics = RankingMetrics.of(predictionAndLabels);
    Assert.assertEquals(0.355026, metrics.meanAveragePrecision(), 1e-5);
    Assert.assertEquals(0.75 / 3.0, metrics.precisionAt(4), 1e-5);
  }
}
