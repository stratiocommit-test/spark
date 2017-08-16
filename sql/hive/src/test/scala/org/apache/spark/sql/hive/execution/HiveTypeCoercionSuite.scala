/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.catalyst.expressions.{Cast, EqualTo}
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.hive.test.TestHive

/**
 * A set of tests that validate type promotion and coercion rules.
 */
class HiveTypeCoercionSuite extends HiveComparisonTest {
  val baseTypes = Seq(
    ("1", "1"),
    ("1.0", "CAST(1.0 AS DOUBLE)"),
    ("1L", "1L"),
    ("1S", "1S"),
    ("1Y", "1Y"),
    ("'1'", "'1'"))

  baseTypes.foreach { case (ni, si) =>
    baseTypes.foreach { case (nj, sj) =>
      createQueryTest(s"$ni + $nj", s"SELECT $si + $sj FROM src LIMIT 1")
    }
  }

  val nullVal = "null"
  baseTypes.init.foreach { case (i, s) =>
    createQueryTest(s"case when then $i else $nullVal end ",
      s"SELECT case when true then $s else $nullVal end FROM src limit 1")
    createQueryTest(s"case when then $nullVal else $i end ",
      s"SELECT case when true then $nullVal else $s end FROM src limit 1")
  }

  test("[SPARK-2210] boolean cast on boolean value should be removed") {
    val q = "select cast(cast(key=0 as boolean) as boolean) from src"
    val project = TestHive.sql(q).queryExecution.sparkPlan.collect {
      case e: ProjectExec => e
    }.head

    // No cast expression introduced
    project.transformAllExpressions { case c: Cast =>
      fail(s"unexpected cast $c")
      c
    }

    // Only one equality check
    var numEquals = 0
    project.transformAllExpressions { case e: EqualTo =>
      numEquals += 1
      e
    }
    assert(numEquals === 1)
  }
}
