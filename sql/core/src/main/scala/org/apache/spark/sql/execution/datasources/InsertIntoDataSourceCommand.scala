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
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OverwriteOptions}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.sources.InsertableRelation


/**
 * Inserts the results of `query` in to a relation that extends [[InsertableRelation]].
 */
case class InsertIntoDataSourceCommand(
    logicalRelation: LogicalRelation,
    query: LogicalPlan,
    overwrite: OverwriteOptions)
  extends RunnableCommand {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val relation = logicalRelation.relation.asInstanceOf[InsertableRelation]
    val data = Dataset.ofRows(sparkSession, query)
    // Apply the schema of the existing table to the new data.
    val df = sparkSession.internalCreateDataFrame(data.queryExecution.toRdd, logicalRelation.schema)
    relation.insert(df, overwrite.enabled)

    // Invalidate the cache.
    sparkSession.sharedState.cacheManager.invalidateCache(logicalRelation)

    Seq.empty[Row]
  }
}
