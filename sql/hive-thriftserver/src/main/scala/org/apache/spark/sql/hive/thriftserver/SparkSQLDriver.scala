/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.hive.thriftserver

import java.util.{ArrayList => JArrayList, Arrays, List => JList}

import scala.collection.JavaConverters._

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Schema}
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.sql.execution.QueryExecution


private[hive] class SparkSQLDriver(val context: SQLContext = SparkSQLEnv.sqlContext)
  extends Driver
  with Logging {

  private[hive] var tableSchema: Schema = _
  private[hive] var hiveResponse: Seq[String] = _

  override def init(): Unit = {
  }

  private def getResultSetSchema(query: QueryExecution): Schema = {
    val analyzed = query.analyzed
    logDebug(s"Result Schema: ${analyzed.output}")
    if (analyzed.output.isEmpty) {
      new Schema(Arrays.asList(new FieldSchema("Response code", "string", "")), null)
    } else {
      val fieldSchemas = analyzed.output.map { attr =>
        new FieldSchema(attr.name, attr.dataType.catalogString, "")
      }

      new Schema(fieldSchemas.asJava, null)
    }
  }

  override def run(command: String): CommandProcessorResponse = {
    // TODO unify the error code
    try {
      context.sparkContext.setJobDescription(command)
      val execution = context.sessionState.executePlan(context.sql(command).logicalPlan)
      hiveResponse = execution.hiveResultString()
      tableSchema = getResultSetSchema(execution)
      new CommandProcessorResponse(0)
    } catch {
        case ae: AnalysisException =>
          logDebug(s"Failed in [$command]", ae)
          new CommandProcessorResponse(1, ExceptionUtils.getStackTrace(ae), null, ae)
        case cause: Throwable =>
          logError(s"Failed in [$command]", cause)
          new CommandProcessorResponse(1, ExceptionUtils.getStackTrace(cause), null, cause)
    }
  }

  override def close(): Int = {
    hiveResponse = null
    tableSchema = null
    0
  }

  override def getResults(res: JList[_]): Boolean = {
    if (hiveResponse == null) {
      false
    } else {
      res.asInstanceOf[JArrayList[String]].addAll(hiveResponse.asJava)
      hiveResponse = null
      true
    }
  }

  override def getSchema: Schema = tableSchema

  override def destroy() {
    super.destroy()
    hiveResponse = null
    tableSchema = null
  }
}
