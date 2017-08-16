/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{In, ListQuery, OuterReference}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, Project}

/**
 * Unit tests for [[ResolveSubquery]].
 */
class ResolveSubquerySuite extends AnalysisTest {

  val a = 'a.int
  val b = 'b.int
  val t1 = LocalRelation(a)
  val t2 = LocalRelation(b)

  test("SPARK-17251 Improve `OuterReference` to be `NamedExpression`") {
    val expr = Filter(In(a, Seq(ListQuery(Project(Seq(OuterReference(a)), t2)))), t1)
    val m = intercept[AnalysisException] {
      SimpleAnalyzer.ResolveSubquery(expr)
    }.getMessage
    assert(m.contains(
      "Expressions referencing the outer query are not supported outside of WHERE/HAVING clauses"))
  }
}
