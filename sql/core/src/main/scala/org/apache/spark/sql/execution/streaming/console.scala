/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

class ConsoleSink(options: Map[String, String]) extends Sink with Logging {
  // Number of rows to display, by default 20 rows
  private val numRowsToShow = options.get("numRows").map(_.toInt).getOrElse(20)

  // Truncate the displayed data if it is too long, by default it is true
  private val isTruncated = options.get("truncate").map(_.toBoolean).getOrElse(true)

  // Track the batch id
  private var lastBatchId = -1L

  override def addBatch(batchId: Long, data: DataFrame): Unit = synchronized {
    val batchIdStr = if (batchId <= lastBatchId) {
      s"Rerun batch: $batchId"
    } else {
      lastBatchId = batchId
      s"Batch: $batchId"
    }

    // scalastyle:off println
    println("-------------------------------------------")
    println(batchIdStr)
    println("-------------------------------------------")
    // scalastyle:off println
    data.sparkSession.createDataFrame(
      data.sparkSession.sparkContext.parallelize(data.collect()), data.schema)
      .show(numRowsToShow, isTruncated)
  }
}

class ConsoleSinkProvider extends StreamSinkProvider with DataSourceRegister {
  def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    new ConsoleSink(parameters)
  }

  def shortName(): String = "console"
}
