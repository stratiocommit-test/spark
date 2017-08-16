/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.util

import org.apache.spark.SparkFunSuite

class IdentifiableSuite extends SparkFunSuite {

  import IdentifiableSuite.Test

  test("Identifiable") {
    val test0 = new Test("test_0")
    assert(test0.uid === "test_0")

    val test1 = new Test
    assert(test1.uid.startsWith("test_"))
  }
}

object IdentifiableSuite {

  class Test(override val uid: String) extends Identifiable {
    def this() = this(Identifiable.randomUID("test"))
  }

}
