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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.sql.catalyst.JavaTypeInference
import org.apache.spark.sql.types._

class VectorUDTSuite extends SparkFunSuite {

  test("preloaded VectorUDT") {
    val dv1 = Vectors.dense(Array.empty[Double])
    val dv2 = Vectors.dense(1.0, 2.0)
    val sv1 = Vectors.sparse(2, Array.empty, Array.empty)
    val sv2 = Vectors.sparse(2, Array(1), Array(2.0))

    for (v <- Seq(dv1, dv2, sv1, sv2)) {
      val udt = UDTRegistration.getUDTFor(v.getClass.getName).get.newInstance()
        .asInstanceOf[VectorUDT]
      assert(v === udt.deserialize(udt.serialize(v)))
      assert(udt.typeName == "vector")
      assert(udt.simpleString == "vector")
    }
  }

  test("JavaTypeInference with VectorUDT") {
    val (dataType, _) = JavaTypeInference.inferDataType(classOf[LabeledPoint])
    assert(dataType.asInstanceOf[StructType].fields.map(_.dataType)
      === Seq(new VectorUDT, DoubleType))
  }
}
