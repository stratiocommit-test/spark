/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.MetadataBuilder

class RemoveAliasOnlyProjectSuite extends PlanTest with PredicateHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("RemoveAliasOnlyProject", FixedPoint(50), RemoveAliasOnlyProject) :: Nil
  }

  test("all expressions in project list are aliased child output") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = relation.select('a as 'a, 'b as 'b).analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, relation)
  }

  test("all expressions in project list are aliased child output but with different order") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = relation.select('b as 'b, 'a as 'a).analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  test("some expressions in project list are aliased child output") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = relation.select('a as 'a, 'b).analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, relation)
  }

  test("some expressions in project list are aliased child output but with different order") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = relation.select('b as 'b, 'a).analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  test("some expressions in project list are not Alias or Attribute") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = relation.select('a as 'a, 'b + 1).analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  test("some expressions in project list are aliased child output but with metadata") {
    val relation = LocalRelation('a.int, 'b.int)
    val metadata = new MetadataBuilder().putString("x", "y").build()
    val aliasWithMeta = Alias('a, "a")(explicitMetadata = Some(metadata))
    val query = relation.select(aliasWithMeta, 'b).analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }
}
