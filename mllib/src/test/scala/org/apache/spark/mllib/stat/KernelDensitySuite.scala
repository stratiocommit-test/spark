/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.stat

import org.apache.commons.math3.distribution.NormalDistribution

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext

class KernelDensitySuite extends SparkFunSuite with MLlibTestSparkContext {
  test("kernel density single sample") {
    val rdd = sc.parallelize(Array(5.0))
    val evaluationPoints = Array(5.0, 6.0)
    val densities = new KernelDensity().setSample(rdd).setBandwidth(3.0).estimate(evaluationPoints)
    val normal = new NormalDistribution(5.0, 3.0)
    val acceptableErr = 1e-6
    assert(math.abs(densities(0) - normal.density(5.0)) < acceptableErr)
    assert(math.abs(densities(1) - normal.density(6.0)) < acceptableErr)
  }

  test("kernel density multiple samples") {
    val rdd = sc.parallelize(Array(5.0, 10.0))
    val evaluationPoints = Array(5.0, 6.0)
    val densities = new KernelDensity().setSample(rdd).setBandwidth(3.0).estimate(evaluationPoints)
    val normal1 = new NormalDistribution(5.0, 3.0)
    val normal2 = new NormalDistribution(10.0, 3.0)
    val acceptableErr = 1e-6
    assert(math.abs(
      densities(0) - (normal1.density(5.0) + normal2.density(5.0)) / 2) < acceptableErr)
    assert(math.abs(
      densities(1) - (normal1.density(6.0) + normal2.density(6.0)) / 2) < acceptableErr)
  }
}
