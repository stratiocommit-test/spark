/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.streaming.ProcessingTime

class ProcessingTimeSuite extends SparkFunSuite {

  test("create") {
    assert(ProcessingTime(10.seconds).intervalMs === 10 * 1000)
    assert(ProcessingTime.create(10, TimeUnit.SECONDS).intervalMs === 10 * 1000)
    assert(ProcessingTime("1 minute").intervalMs === 60 * 1000)
    assert(ProcessingTime("interval 1 minute").intervalMs === 60 * 1000)

    intercept[IllegalArgumentException] { ProcessingTime(null: String) }
    intercept[IllegalArgumentException] { ProcessingTime("") }
    intercept[IllegalArgumentException] { ProcessingTime("invalid") }
    intercept[IllegalArgumentException] { ProcessingTime("1 month") }
    intercept[IllegalArgumentException] { ProcessingTime("1 year") }
  }
}
