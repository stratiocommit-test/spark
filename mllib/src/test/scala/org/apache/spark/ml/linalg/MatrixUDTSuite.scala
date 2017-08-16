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
import org.apache.spark.sql.types._

class MatrixUDTSuite extends SparkFunSuite {

  test("preloaded MatrixUDT") {
    val dm1 = new DenseMatrix(2, 2, Array(0.9, 1.2, 2.3, 9.8))
    val dm2 = new DenseMatrix(3, 2, Array(0.0, 1.21, 2.3, 9.8, 9.0, 0.0))
    val dm3 = new DenseMatrix(0, 0, Array())
    val sm1 = dm1.toSparse
    val sm2 = dm2.toSparse
    val sm3 = dm3.toSparse

    for (m <- Seq(dm1, dm2, dm3, sm1, sm2, sm3)) {
      val udt = UDTRegistration.getUDTFor(m.getClass.getName).get.newInstance()
        .asInstanceOf[MatrixUDT]
      assert(m === udt.deserialize(udt.serialize(m)))
      assert(udt.typeName == "matrix")
      assert(udt.simpleString == "matrix")
    }
  }
}
