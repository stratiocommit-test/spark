/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark

import scala.concurrent.duration.Duration

import org.scalatest.{BeforeAndAfter, Matchers}

import org.apache.spark.util.ThreadUtils


class FutureActionSuite
  extends SparkFunSuite
  with BeforeAndAfter
  with Matchers
  with LocalSparkContext {

  before {
    sc = new SparkContext("local", "FutureActionSuite")
  }

  test("simple async action") {
    val rdd = sc.parallelize(1 to 10, 2)
    val job = rdd.countAsync()
    val res = ThreadUtils.awaitResult(job, Duration.Inf)
    res should be (10)
    job.jobIds.size should be (1)
  }

  test("complex async action") {
    val rdd = sc.parallelize(1 to 15, 3)
    val job = rdd.takeAsync(10)
    val res = ThreadUtils.awaitResult(job, Duration.Inf)
    res should be (1 to 10)
    job.jobIds.size should be (2)
  }

}
