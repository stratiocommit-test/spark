/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.types.{IntegerType, StringType}

class ScalaUDFSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("basic") {
    val intUdf = ScalaUDF((i: Int) => i + 1, IntegerType, Literal(1) :: Nil)
    checkEvaluation(intUdf, 2)

    val stringUdf = ScalaUDF((s: String) => s + "x", StringType, Literal("a") :: Nil)
    checkEvaluation(stringUdf, "ax")
  }

  test("better error message for NPE") {
    val udf = ScalaUDF(
      (s: String) => s.toLowerCase,
      StringType,
      Literal.create(null, StringType) :: Nil)

    val e1 = intercept[SparkException](udf.eval())
    assert(e1.getMessage.contains("Failed to execute user defined function"))

    val e2 = intercept[SparkException] {
      checkEvalutionWithUnsafeProjection(udf, null)
    }
    assert(e2.getMessage.contains("Failed to execute user defined function"))
  }

}
