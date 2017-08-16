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

import org.apache.spark.sql.catalyst.analysis.TestRelations.testRelation2
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.SimpleCatalystConf

class SubstituteUnresolvedOrdinalsSuite extends AnalysisTest {
  private lazy val conf = SimpleCatalystConf(caseSensitiveAnalysis = true)
  private lazy val a = testRelation2.output(0)
  private lazy val b = testRelation2.output(1)

  test("unresolved ordinal should not be unresolved") {
    // Expression OrderByOrdinal is unresolved.
    assert(!UnresolvedOrdinal(0).resolved)
  }

  test("order by ordinal") {
    // Tests order by ordinal, apply single rule.
    val plan = testRelation2.orderBy(Literal(1).asc, Literal(2).asc)
    comparePlans(
      new SubstituteUnresolvedOrdinals(conf).apply(plan),
      testRelation2.orderBy(UnresolvedOrdinal(1).asc, UnresolvedOrdinal(2).asc))

    // Tests order by ordinal, do full analysis
    checkAnalysis(plan, testRelation2.orderBy(a.asc, b.asc))

    // order by ordinal can be turned off by config
    comparePlans(
      new SubstituteUnresolvedOrdinals(conf.copy(orderByOrdinal = false)).apply(plan),
      testRelation2.orderBy(Literal(1).asc, Literal(2).asc))
  }

  test("group by ordinal") {
    // Tests group by ordinal, apply single rule.
    val plan2 = testRelation2.groupBy(Literal(1), Literal(2))('a, 'b)
    comparePlans(
      new SubstituteUnresolvedOrdinals(conf).apply(plan2),
      testRelation2.groupBy(UnresolvedOrdinal(1), UnresolvedOrdinal(2))('a, 'b))

    // Tests group by ordinal, do full analysis
    checkAnalysis(plan2, testRelation2.groupBy(a, b)(a, b))

    // group by ordinal can be turned off by config
    comparePlans(
      new SubstituteUnresolvedOrdinals(conf.copy(groupByOrdinal = false)).apply(plan2),
      testRelation2.groupBy(Literal(1), Literal(2))('a, 'b))
  }
}
