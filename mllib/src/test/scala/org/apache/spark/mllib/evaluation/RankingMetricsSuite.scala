/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.evaluation

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class RankingMetricsSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("Ranking metrics: MAP, NDCG") {
    val predictionAndLabels = sc.parallelize(
      Seq(
        (Array(1, 6, 2, 7, 8, 3, 9, 10, 4, 5), Array(1, 2, 3, 4, 5)),
        (Array(4, 1, 5, 6, 2, 7, 3, 8, 9, 10), Array(1, 2, 3)),
        (Array(1, 2, 3, 4, 5), Array.empty[Int])
      ), 2)
    val eps = 1.0E-5

    val metrics = new RankingMetrics(predictionAndLabels)
    val map = metrics.meanAveragePrecision

    assert(metrics.precisionAt(1) ~== 1.0/3 absTol eps)
    assert(metrics.precisionAt(2) ~== 1.0/3 absTol eps)
    assert(metrics.precisionAt(3) ~== 1.0/3 absTol eps)
    assert(metrics.precisionAt(4) ~== 0.75/3 absTol eps)
    assert(metrics.precisionAt(5) ~== 0.8/3 absTol eps)
    assert(metrics.precisionAt(10) ~== 0.8/3 absTol eps)
    assert(metrics.precisionAt(15) ~== 8.0/45 absTol eps)

    assert(map ~== 0.355026 absTol eps)

    assert(metrics.ndcgAt(3) ~== 1.0/3 absTol eps)
    assert(metrics.ndcgAt(5) ~== 0.328788 absTol eps)
    assert(metrics.ndcgAt(10) ~== 0.487913 absTol eps)
    assert(metrics.ndcgAt(15) ~== metrics.ndcgAt(10) absTol eps)
  }

  test("MAP, NDCG with few predictions (SPARK-14886)") {
    val predictionAndLabels = sc.parallelize(
      Seq(
        (Array(1, 6, 2), Array(1, 2, 3, 4, 5)),
        (Array.empty[Int], Array(1, 2, 3))
      ), 2)
    val eps = 1.0E-5

    val metrics = new RankingMetrics(predictionAndLabels)
    assert(metrics.precisionAt(1) ~== 0.5 absTol eps)
    assert(metrics.precisionAt(2) ~== 0.25 absTol eps)
    assert(metrics.ndcgAt(1) ~== 0.5 absTol eps)
    assert(metrics.ndcgAt(2) ~== 0.30657 absTol eps)
  }

}
