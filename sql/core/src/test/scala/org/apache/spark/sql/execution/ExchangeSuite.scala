/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, IdentityBroadcastMode, SinglePartition}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchange}
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.test.SharedSQLContext

class ExchangeSuite extends SparkPlanTest with SharedSQLContext {
  import testImplicits._

  test("shuffling UnsafeRows in exchange") {
    val input = (1 to 1000).map(Tuple1.apply)
    checkAnswer(
      input.toDF(),
      plan => ShuffleExchange(SinglePartition, plan),
      input.map(Row.fromTuple)
    )
  }

  test("compatible BroadcastMode") {
    val mode1 = IdentityBroadcastMode
    val mode2 = HashedRelationBroadcastMode(Literal(1L) :: Nil)
    val mode3 = HashedRelationBroadcastMode(Literal("s") :: Nil)

    assert(mode1.compatibleWith(mode1))
    assert(!mode1.compatibleWith(mode2))
    assert(!mode2.compatibleWith(mode1))
    assert(mode2.compatibleWith(mode2))
    assert(!mode2.compatibleWith(mode3))
    assert(mode3.compatibleWith(mode3))
  }

  test("BroadcastExchange same result") {
    val df = spark.range(10)
    val plan = df.queryExecution.executedPlan
    val output = plan.output
    assert(plan sameResult plan)

    val exchange1 = BroadcastExchangeExec(IdentityBroadcastMode, plan)
    val hashMode = HashedRelationBroadcastMode(output)
    val exchange2 = BroadcastExchangeExec(hashMode, plan)
    val hashMode2 =
      HashedRelationBroadcastMode(Alias(output.head, "id2")() :: Nil)
    val exchange3 = BroadcastExchangeExec(hashMode2, plan)
    val exchange4 = ReusedExchangeExec(output, exchange3)

    assert(exchange1 sameResult exchange1)
    assert(exchange2 sameResult exchange2)
    assert(exchange3 sameResult exchange3)
    assert(exchange4 sameResult exchange4)

    assert(!exchange1.sameResult(exchange2))
    assert(!exchange2.sameResult(exchange3))
    assert(!exchange3.sameResult(exchange4))
    assert(exchange4 sameResult exchange3)
  }

  test("ShuffleExchange same result") {
    val df = spark.range(10)
    val plan = df.queryExecution.executedPlan
    val output = plan.output
    assert(plan sameResult plan)

    val part1 = HashPartitioning(output, 1)
    val exchange1 = ShuffleExchange(part1, plan)
    val exchange2 = ShuffleExchange(part1, plan)
    val part2 = HashPartitioning(output, 2)
    val exchange3 = ShuffleExchange(part2, plan)
    val part3 = HashPartitioning(output ++ output, 2)
    val exchange4 = ShuffleExchange(part3, plan)
    val exchange5 = ReusedExchangeExec(output, exchange4)

    assert(exchange1 sameResult exchange1)
    assert(exchange2 sameResult exchange2)
    assert(exchange3 sameResult exchange3)
    assert(exchange4 sameResult exchange4)
    assert(exchange5 sameResult exchange5)

    assert(exchange1 sameResult exchange2)
    assert(!exchange2.sameResult(exchange3))
    assert(!exchange3.sameResult(exchange4))
    assert(!exchange4.sameResult(exchange5))
    assert(exchange5 sameResult exchange4)
  }
}
