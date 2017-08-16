/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.streaming.util

import java.util.concurrent.CountDownLatch

import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Sink, Source}
import org.apache.spark.sql.sources.{StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/** Dummy provider: returns a SourceProvider with a blocking `createSource` call. */
class BlockingSource extends StreamSourceProvider with StreamSinkProvider {

  private val fakeSchema = StructType(StructField("a", IntegerType) :: Nil)

  override def sourceSchema(
      spark: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    ("dummySource", fakeSchema)
  }

  override def createSource(
      spark: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    BlockingSource.latch.await()
    new Source {
      override def schema: StructType = fakeSchema
      override def getOffset: Option[Offset] = Some(new LongOffset(0))
      override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        import spark.implicits._
        Seq[Int]().toDS().toDF()
      }
      override def stop() {}
    }
  }

  override def createSink(
      spark: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    new Sink {
      override def addBatch(batchId: Long, data: DataFrame): Unit = {}
    }
  }
}

object BlockingSource {
  var latch: CountDownLatch = null
}
