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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class OptimizeMetadataOnlyQuerySuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    val data = (1 to 10).map(i => (i, s"data-$i", i % 2, if ((i % 2) == 0) "even" else "odd"))
      .toDF("col1", "col2", "partcol1", "partcol2")
    data.write.partitionBy("partcol1", "partcol2").mode("append").saveAsTable("srcpart")
  }

  override protected def afterAll(): Unit = {
    try {
      sql("DROP TABLE IF EXISTS srcpart")
    } finally {
      super.afterAll()
    }
  }

  private def assertMetadataOnlyQuery(df: DataFrame): Unit = {
    val localRelations = df.queryExecution.optimizedPlan.collect {
      case l @ LocalRelation(_, _) => l
    }
    assert(localRelations.size == 1)
  }

  private def assertNotMetadataOnlyQuery(df: DataFrame): Unit = {
    val localRelations = df.queryExecution.optimizedPlan.collect {
      case l @ LocalRelation(_, _) => l
    }
    assert(localRelations.size == 0)
  }

  private def testMetadataOnly(name: String, sqls: String*): Unit = {
    test(name) {
      withSQLConf(SQLConf.OPTIMIZER_METADATA_ONLY.key -> "true") {
        sqls.foreach { case q => assertMetadataOnlyQuery(sql(q)) }
      }
      withSQLConf(SQLConf.OPTIMIZER_METADATA_ONLY.key -> "false") {
        sqls.foreach { case q => assertNotMetadataOnlyQuery(sql(q)) }
      }
    }
  }

  private def testNotMetadataOnly(name: String, sqls: String*): Unit = {
    test(name) {
      withSQLConf(SQLConf.OPTIMIZER_METADATA_ONLY.key -> "true") {
        sqls.foreach { case q => assertNotMetadataOnlyQuery(sql(q)) }
      }
      withSQLConf(SQLConf.OPTIMIZER_METADATA_ONLY.key -> "false") {
        sqls.foreach { case q => assertNotMetadataOnlyQuery(sql(q)) }
      }
    }
  }

  testMetadataOnly(
    "Aggregate expression is partition columns",
    "select partcol1 from srcpart group by partcol1",
    "select partcol2 from srcpart where partcol1 = 0 group by partcol2")

  testMetadataOnly(
    "Distinct aggregate function on partition columns",
    "SELECT partcol1, count(distinct partcol2) FROM srcpart group by partcol1",
    "SELECT partcol1, count(distinct partcol2) FROM srcpart where partcol1 = 0 group by partcol1")

  testMetadataOnly(
    "Distinct on partition columns",
    "select distinct partcol1, partcol2 from srcpart",
    "select distinct c1 from (select partcol1 + 1 as c1 from srcpart where partcol1 = 0) t")

  testMetadataOnly(
    "Aggregate function on partition columns which have same result w or w/o DISTINCT keyword",
    "select max(partcol1) from srcpart",
    "select min(partcol1) from srcpart where partcol1 = 0",
    "select first(partcol1) from srcpart",
    "select last(partcol1) from srcpart where partcol1 = 0",
    "select partcol2, min(partcol1) from srcpart where partcol1 = 0 group by partcol2",
    "select max(c1) from (select partcol1 + 1 as c1 from srcpart where partcol1 = 0) t")

  testNotMetadataOnly(
    "Don't optimize metadata only query for non-partition columns",
    "select col1 from srcpart group by col1",
    "select partcol1, max(col1) from srcpart group by partcol1",
    "select partcol1, count(distinct col1) from srcpart group by partcol1",
    "select distinct partcol1, col1 from srcpart")

  testNotMetadataOnly(
    "Don't optimize metadata only query for non-distinct aggregate function on partition columns",
    "select partcol1, sum(partcol2) from srcpart group by partcol1",
    "select partcol1, count(partcol2) from srcpart group by partcol1")

  testNotMetadataOnly(
    "Don't optimize metadata only query for GroupingSet/Union operator",
    "select partcol1, max(partcol2) from srcpart where partcol1 = 0 group by rollup (partcol1)",
    "select partcol2 from (select partcol2 from srcpart where partcol1 = 0 union all " +
      "select partcol2 from srcpart where partcol1 = 1) t group by partcol2")
}
