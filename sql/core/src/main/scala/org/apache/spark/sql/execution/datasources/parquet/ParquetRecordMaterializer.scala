/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.datasources.parquet

import org.apache.parquet.io.api.{GroupConverter, RecordMaterializer}
import org.apache.parquet.schema.MessageType

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.StructType

/**
 * A [[RecordMaterializer]] for Catalyst rows.
 *
 * @param parquetSchema Parquet schema of the records to be read
 * @param catalystSchema Catalyst schema of the rows to be constructed
 * @param schemaConverter A Parquet-Catalyst schema converter that helps initializing row converters
 */
private[parquet] class ParquetRecordMaterializer(
    parquetSchema: MessageType, catalystSchema: StructType, schemaConverter: ParquetSchemaConverter)
  extends RecordMaterializer[UnsafeRow] {

  private val rootConverter =
    new ParquetRowConverter(schemaConverter, parquetSchema, catalystSchema, NoopUpdater)

  override def getCurrentRecord: UnsafeRow = rootConverter.currentRecord

  override def getRootConverter: GroupConverter = rootConverter
}
