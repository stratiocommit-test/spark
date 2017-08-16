/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.tree.impl

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.tree.EnsembleTestHelper
import org.apache.spark.mllib.util.MLlibTestSparkContext

/**
 * Test suite for [[BaggedPoint]].
 */
class BaggedPointSuite extends SparkFunSuite with MLlibTestSparkContext  {

  test("BaggedPoint RDD: without subsampling") {
    val arr = EnsembleTestHelper.generateOrderedLabeledPoints(1, 1000)
    val rdd = sc.parallelize(arr)
    val baggedRDD = BaggedPoint.convertToBaggedRDD(rdd, 1.0, 1, false, 42)
    baggedRDD.collect().foreach { baggedPoint =>
      assert(baggedPoint.subsampleWeights.size == 1 && baggedPoint.subsampleWeights(0) == 1)
    }
  }

  test("BaggedPoint RDD: with subsampling with replacement (fraction = 1.0)") {
    val numSubsamples = 100
    val (expectedMean, expectedStddev) = (1.0, 1.0)

    val seeds = Array(123, 5354, 230, 349867, 23987)
    val arr = EnsembleTestHelper.generateOrderedLabeledPoints(1, 1000)
    val rdd = sc.parallelize(arr)
    seeds.foreach { seed =>
      val baggedRDD = BaggedPoint.convertToBaggedRDD(rdd, 1.0, numSubsamples, true, seed)
      val subsampleCounts: Array[Array[Double]] = baggedRDD.map(_.subsampleWeights).collect()
      EnsembleTestHelper.testRandomArrays(subsampleCounts, numSubsamples, expectedMean,
        expectedStddev, epsilon = 0.01)
    }
  }

  test("BaggedPoint RDD: with subsampling with replacement (fraction = 0.5)") {
    val numSubsamples = 100
    val subsample = 0.5
    val (expectedMean, expectedStddev) = (subsample, math.sqrt(subsample))

    val seeds = Array(123, 5354, 230, 349867, 23987)
    val arr = EnsembleTestHelper.generateOrderedLabeledPoints(1, 1000)
    val rdd = sc.parallelize(arr)
    seeds.foreach { seed =>
      val baggedRDD = BaggedPoint.convertToBaggedRDD(rdd, subsample, numSubsamples, true, seed)
      val subsampleCounts: Array[Array[Double]] = baggedRDD.map(_.subsampleWeights).collect()
      EnsembleTestHelper.testRandomArrays(subsampleCounts, numSubsamples, expectedMean,
        expectedStddev, epsilon = 0.01)
    }
  }

  test("BaggedPoint RDD: with subsampling without replacement (fraction = 1.0)") {
    val numSubsamples = 100
    val (expectedMean, expectedStddev) = (1.0, 0)

    val seeds = Array(123, 5354, 230, 349867, 23987)
    val arr = EnsembleTestHelper.generateOrderedLabeledPoints(1, 1000)
    val rdd = sc.parallelize(arr)
    seeds.foreach { seed =>
      val baggedRDD = BaggedPoint.convertToBaggedRDD(rdd, 1.0, numSubsamples, false, seed)
      val subsampleCounts: Array[Array[Double]] = baggedRDD.map(_.subsampleWeights).collect()
      EnsembleTestHelper.testRandomArrays(subsampleCounts, numSubsamples, expectedMean,
        expectedStddev, epsilon = 0.01)
    }
  }

  test("BaggedPoint RDD: with subsampling without replacement (fraction = 0.5)") {
    val numSubsamples = 100
    val subsample = 0.5
    val (expectedMean, expectedStddev) = (subsample, math.sqrt(subsample * (1 - subsample)))

    val seeds = Array(123, 5354, 230, 349867, 23987)
    val arr = EnsembleTestHelper.generateOrderedLabeledPoints(1, 1000)
    val rdd = sc.parallelize(arr)
    seeds.foreach { seed =>
      val baggedRDD = BaggedPoint.convertToBaggedRDD(rdd, subsample, numSubsamples, false, seed)
      val subsampleCounts: Array[Array[Double]] = baggedRDD.map(_.subsampleWeights).collect()
      EnsembleTestHelper.testRandomArrays(subsampleCounts, numSubsamples, expectedMean,
        expectedStddev, epsilon = 0.01)
    }
  }
}
