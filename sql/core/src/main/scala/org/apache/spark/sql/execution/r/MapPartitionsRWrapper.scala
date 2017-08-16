/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.r

import org.apache.spark.api.r._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.api.r.SQLUtils._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
 * A function wrapper that applies the given R function to each partition.
 */
case class MapPartitionsRWrapper(
    func: Array[Byte],
    packageNames: Array[Byte],
    broadcastVars: Array[Broadcast[Object]],
    inputSchema: StructType,
    outputSchema: StructType) extends (Iterator[Any] => Iterator[Any]) {
  def apply(iter: Iterator[Any]): Iterator[Any] = {
    // If the content of current DataFrame is serialized R data?
    val isSerializedRData =
      if (inputSchema == SERIALIZED_R_DATA_SCHEMA) true else false

    val (newIter, deserializer, colNames) =
      if (!isSerializedRData) {
        // Serialize each row into a byte array that can be deserialized in the R worker
        (iter.asInstanceOf[Iterator[Row]].map {row => rowToRBytes(row)},
         SerializationFormats.ROW, inputSchema.fieldNames)
      } else {
        (iter.asInstanceOf[Iterator[Row]].map { row => row(0) }, SerializationFormats.BYTE, null)
      }

    val serializer = if (outputSchema != SERIALIZED_R_DATA_SCHEMA) {
      SerializationFormats.ROW
    } else {
      SerializationFormats.BYTE
    }

    val runner = new RRunner[Array[Byte]](
      func, deserializer, serializer, packageNames, broadcastVars,
      isDataFrame = true, colNames = colNames, mode = RRunnerModes.DATAFRAME_DAPPLY)
    // Partition index is ignored. Dataset has no support for mapPartitionsWithIndex.
    val outputIter = runner.compute(newIter, -1)

    if (serializer == SerializationFormats.ROW) {
      outputIter.map { bytes => bytesToRow(bytes, outputSchema) }
    } else {
      outputIter.map { bytes => Row.fromSeq(Seq(bytes)) }
    }
  }
}
