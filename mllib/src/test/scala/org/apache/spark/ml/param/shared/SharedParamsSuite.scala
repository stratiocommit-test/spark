/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.param.shared

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.{ParamMap, Params}

class SharedParamsSuite extends SparkFunSuite {

  test("outputCol") {

    class Obj(override val uid: String) extends Params with HasOutputCol {
      override def copy(extra: ParamMap): Obj = defaultCopy(extra)
    }

    val obj = new Obj("obj")

    assert(obj.hasDefault(obj.outputCol))
    assert(obj.getOrDefault(obj.outputCol) === "obj__output")
  }
}
