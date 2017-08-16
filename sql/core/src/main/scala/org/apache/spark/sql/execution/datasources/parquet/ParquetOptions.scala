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

import org.apache.parquet.hadoop.metadata.CompressionCodecName

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf

/**
 * Options for the Parquet data source.
 */
private[parquet] class ParquetOptions(
    @transient private val parameters: CaseInsensitiveMap,
    @transient private val sqlConf: SQLConf)
  extends Serializable {

  import ParquetOptions._

  def this(parameters: Map[String, String], sqlConf: SQLConf) =
    this(new CaseInsensitiveMap(parameters), sqlConf)

  /**
   * Compression codec to use. By default use the value specified in SQLConf.
   * Acceptable values are defined in [[shortParquetCompressionCodecNames]].
   */
  val compressionCodecClassName: String = {
    val codecName = parameters.getOrElse("compression", sqlConf.parquetCompressionCodec).toLowerCase
    if (!shortParquetCompressionCodecNames.contains(codecName)) {
      val availableCodecs = shortParquetCompressionCodecNames.keys.map(_.toLowerCase)
      throw new IllegalArgumentException(s"Codec [$codecName] " +
        s"is not available. Available codecs are ${availableCodecs.mkString(", ")}.")
    }
    shortParquetCompressionCodecNames(codecName).name()
  }

  /**
   * Whether it merges schemas or not. When the given Parquet files have different schemas,
   * the schemas can be merged.  By default use the value specified in SQLConf.
   */
  val mergeSchema: Boolean = parameters
    .get(MERGE_SCHEMA)
    .map(_.toBoolean)
    .getOrElse(sqlConf.isParquetSchemaMergingEnabled)
}


object ParquetOptions {
  val MERGE_SCHEMA = "mergeSchema"

  // The parquet compression short names
  private val shortParquetCompressionCodecNames = Map(
    "none" -> CompressionCodecName.UNCOMPRESSED,
    "uncompressed" -> CompressionCodecName.UNCOMPRESSED,
    "snappy" -> CompressionCodecName.SNAPPY,
    "gzip" -> CompressionCodecName.GZIP,
    "lzo" -> CompressionCodecName.LZO)
}
