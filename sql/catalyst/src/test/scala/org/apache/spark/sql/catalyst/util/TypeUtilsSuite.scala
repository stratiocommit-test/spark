/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.types._

class TypeUtilsSuite extends SparkFunSuite {

  private def typeCheckPass(types: Seq[DataType]): Unit = {
    assert(TypeUtils.checkForSameTypeInputExpr(types, "a") == TypeCheckSuccess)
  }

  private def typeCheckFail(types: Seq[DataType]): Unit = {
    assert(TypeUtils.checkForSameTypeInputExpr(types, "a").isInstanceOf[TypeCheckFailure])
  }

  test("checkForSameTypeInputExpr") {
    typeCheckPass(Nil)
    typeCheckPass(StringType :: Nil)
    typeCheckPass(StringType :: StringType :: Nil)

    typeCheckFail(StringType :: IntegerType :: Nil)
    typeCheckFail(StringType :: IntegerType :: Nil)

    // Should also work on arrays. See SPARK-14990
    typeCheckPass(ArrayType(StringType, containsNull = true) ::
      ArrayType(StringType, containsNull = false) :: Nil)
  }
}
