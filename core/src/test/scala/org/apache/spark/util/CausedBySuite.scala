/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.util

import org.apache.spark.SparkFunSuite

class CausedBySuite extends SparkFunSuite {

  test("For an error without a cause, should return the error") {
    val error = new Exception

    val causedBy = error match {
      case CausedBy(e) => e
    }

    assert(causedBy === error)
  }

  test("For an error with a cause, should return the cause of the error") {
    val cause = new Exception
    val error = new Exception(cause)

    val causedBy = error match {
      case CausedBy(e) => e
    }

    assert(causedBy === cause)
  }

  test("For an error with a cause that itself has a cause, return the root cause") {
    val causeOfCause = new Exception
    val cause = new Exception(causeOfCause)
    val error = new Exception(cause)

    val causedBy = error match {
      case CausedBy(e) => e
    }

    assert(causedBy === causeOfCause)
  }
}
