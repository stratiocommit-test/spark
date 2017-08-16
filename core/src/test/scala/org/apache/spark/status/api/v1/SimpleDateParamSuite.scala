/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.status.api.v1

import javax.ws.rs.WebApplicationException

import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite

class SimpleDateParamSuite extends SparkFunSuite with Matchers {

  test("date parsing") {
    new SimpleDateParam("2015-02-20T23:21:17.190GMT").timestamp should be (1424474477190L)
    // don't use EST, it is ambiguous, use -0500 instead, see SPARK-15723
    new SimpleDateParam("2015-02-20T17:21:17.190-0500").timestamp should be (1424470877190L)
    new SimpleDateParam("2015-02-20").timestamp should be (1424390400000L) // GMT
    intercept[WebApplicationException] {
      new SimpleDateParam("invalid date")
    }
  }

}
