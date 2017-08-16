/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.linalg

import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.SparkFunSuite

class JsonVectorConverterSuite extends SparkFunSuite {

  test("toJson/fromJson") {
    val sv0 = Vectors.sparse(0, Array.empty, Array.empty)
    val sv1 = Vectors.sparse(1, Array.empty, Array.empty)
    val sv2 = Vectors.sparse(2, Array(1), Array(2.0))
    val dv0 = Vectors.dense(Array.empty[Double])
    val dv1 = Vectors.dense(1.0)
    val dv2 = Vectors.dense(0.0, 2.0)
    for (v <- Seq(sv0, sv1, sv2, dv0, dv1, dv2)) {
      val json = JsonVectorConverter.toJson(v)
      parse(json) // `json` should be a valid JSON string
      val u = JsonVectorConverter.fromJson(json)
      assert(u.getClass === v.getClass, "toJson/fromJson should preserve vector types.")
      assert(u === v, "toJson/fromJson should preserve vector values.")
    }
  }
}
