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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.test.TestHiveSingleton

class HivePlanTest extends QueryTest with TestHiveSingleton {
  import spark.sql
  import spark.implicits._

  test("udf constant folding") {
    Seq.empty[Tuple1[Int]].toDF("a").createOrReplaceTempView("t")
    val optimized = sql("SELECT cos(null) AS c FROM t").queryExecution.optimizedPlan
    val correctAnswer = sql("SELECT cast(null as double) AS c FROM t").queryExecution.optimizedPlan

    comparePlans(optimized, correctAnswer)
  }

  test("window expressions sharing the same partition by and order by clause") {
    val df = Seq.empty[(Int, String, Int, Int)].toDF("id", "grp", "seq", "val")
    val window = Window.
      partitionBy($"grp").
      orderBy($"val")
    val query = df.select(
      $"id",
      sum($"val").over(window.rowsBetween(-1, 1)),
      sum($"val").over(window.rangeBetween(-1, 1))
    )
    val plan = query.queryExecution.analyzed
    assert(plan.collect{ case w: logical.Window => w }.size === 1,
      "Should have only 1 Window operator.")
  }
}
