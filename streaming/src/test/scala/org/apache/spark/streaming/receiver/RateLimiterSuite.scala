/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.receiver

import org.apache.spark.SparkConf
import org.apache.spark.SparkFunSuite

/** Testsuite for testing the network receiver behavior */
class RateLimiterSuite extends SparkFunSuite {

  test("rate limiter initializes even without a maxRate set") {
    val conf = new SparkConf()
    val rateLimiter = new RateLimiter(conf) {}
    rateLimiter.updateRate(105)
    assert(rateLimiter.getCurrentLimit == 105)
  }

  test("rate limiter updates when below maxRate") {
    val conf = new SparkConf().set("spark.streaming.receiver.maxRate", "110")
    val rateLimiter = new RateLimiter(conf) {}
    rateLimiter.updateRate(105)
    assert(rateLimiter.getCurrentLimit == 105)
  }

  test("rate limiter stays below maxRate despite large updates") {
    val conf = new SparkConf().set("spark.streaming.receiver.maxRate", "100")
    val rateLimiter = new RateLimiter(conf) {}
    rateLimiter.updateRate(105)
    assert(rateLimiter.getCurrentLimit === 100)
  }
}
