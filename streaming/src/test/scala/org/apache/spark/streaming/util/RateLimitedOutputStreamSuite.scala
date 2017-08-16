/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.util

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit._

import org.apache.spark.SparkFunSuite

class RateLimitedOutputStreamSuite extends SparkFunSuite {

  private def benchmark[U](f: => U): Long = {
    val start = System.nanoTime
    f
    System.nanoTime - start
  }

  test("write") {
    val underlying = new ByteArrayOutputStream
    val data = "X" * 41000
    val stream = new RateLimitedOutputStream(underlying, desiredBytesPerSec = 10000)
    val elapsedNs = benchmark { stream.write(data.getBytes(StandardCharsets.UTF_8)) }

    val seconds = SECONDS.convert(elapsedNs, NANOSECONDS)
    assert(seconds >= 4, s"Seconds value ($seconds) is less than 4.")
    assert(seconds <= 30, s"Took more than 30 seconds ($seconds) to write data.")
    assert(underlying.toString("UTF-8") === data)
  }
}
