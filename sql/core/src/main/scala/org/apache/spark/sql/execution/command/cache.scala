/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

case class CacheTableCommand(
    tableIdent: TableIdentifier,
    plan: Option[LogicalPlan],
    isLazy: Boolean) extends RunnableCommand {
  require(plan.isEmpty || tableIdent.database.isEmpty,
    "Database name is not allowed in CACHE TABLE AS SELECT")

  override protected def innerChildren: Seq[QueryPlan[_]] = {
    plan.toSeq
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    plan.foreach { logicalPlan =>
      Dataset.ofRows(sparkSession, logicalPlan).createTempView(tableIdent.quotedString)
    }
    sparkSession.catalog.cacheTable(tableIdent.quotedString)

    if (!isLazy) {
      // Performs eager caching
      sparkSession.table(tableIdent).count()
    }

    Seq.empty[Row]
  }
}


case class UncacheTableCommand(
    tableIdent: TableIdentifier,
    ifExists: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tableId = tableIdent.quotedString
    try {
      sparkSession.catalog.uncacheTable(tableId)
    } catch {
      case _: NoSuchTableException if ifExists => // don't throw
    }
    Seq.empty[Row]
  }
}

/**
 * Clear all cached data from the in-memory cache.
 */
case object ClearCacheCommand extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.catalog.clearCache()
    Seq.empty[Row]
  }
}
