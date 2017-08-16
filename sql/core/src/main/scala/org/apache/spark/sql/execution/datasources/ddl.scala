/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types._

case class CreateTable(
    tableDesc: CatalogTable,
    mode: SaveMode,
    query: Option[LogicalPlan]) extends Command {
  assert(tableDesc.provider.isDefined, "The table to be created must have a provider.")

  if (query.isEmpty) {
    assert(
      mode == SaveMode.ErrorIfExists || mode == SaveMode.Ignore,
      "create table without data insertion can only use ErrorIfExists or Ignore as SaveMode.")
  }

  override def innerChildren: Seq[QueryPlan[_]] = query.toSeq
}

/**
 * Create or replace a local/global temporary view with given data source.
 */
case class CreateTempViewUsing(
    tableIdent: TableIdentifier,
    userSpecifiedSchema: Option[StructType],
    replace: Boolean,
    global: Boolean,
    provider: String,
    options: Map[String, String]) extends RunnableCommand {

  if (tableIdent.database.isDefined) {
    throw new AnalysisException(
      s"Temporary view '$tableIdent' should not have specified a database")
  }

  override def argString: String = {
    s"[tableIdent:$tableIdent " +
      userSpecifiedSchema.map(_ + " ").getOrElse("") +
      s"replace:$replace " +
      s"provider:$provider " +
      CatalogUtils.maskCredentials(options)
  }

  def run(sparkSession: SparkSession): Seq[Row] = {
    val dataSource = DataSource(
      sparkSession,
      userSpecifiedSchema = userSpecifiedSchema,
      className = provider,
      options = options)

    val catalog = sparkSession.sessionState.catalog
    val viewDefinition = Dataset.ofRows(
      sparkSession, LogicalRelation(dataSource.resolveRelation())).logicalPlan

    if (global) {
      catalog.createGlobalTempView(tableIdent.table, viewDefinition, replace)
    } else {
      catalog.createTempView(tableIdent.table, viewDefinition, replace)
    }

    Seq.empty[Row]
  }
}

case class RefreshTable(tableIdent: TableIdentifier)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Refresh the given table's metadata. If this table is cached as an InMemoryRelation,
    // drop the original cached version and make the new version cached lazily.
    sparkSession.catalog.refreshTable(tableIdent.quotedString)
    Seq.empty[Row]
  }
}

case class RefreshResource(path: String)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.catalog.refreshByPath(path)
    Seq.empty[Row]
  }
}
