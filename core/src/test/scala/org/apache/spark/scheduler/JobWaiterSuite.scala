/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.scheduler

import scala.util.Failure

import org.apache.spark.SparkFunSuite

class JobWaiterSuite extends SparkFunSuite {

  test("call jobFailed multiple times") {
    val waiter = new JobWaiter[Int](null, 0, totalTasks = 2, null)

    // Should not throw exception if calling jobFailed multiple times
    waiter.jobFailed(new RuntimeException("Oops 1"))
    waiter.jobFailed(new RuntimeException("Oops 2"))
    waiter.jobFailed(new RuntimeException("Oops 3"))

    waiter.completionFuture.value match {
      case Some(Failure(e)) =>
        // We should receive the first exception
        assert("Oops 1" === e.getMessage)
      case other => fail("Should receiver the first exception but it was " + other)
    }
  }
}
