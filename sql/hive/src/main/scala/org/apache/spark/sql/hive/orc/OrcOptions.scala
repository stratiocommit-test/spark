/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.hive.orc

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Options for the ORC data source.
 */
private[orc] class OrcOptions(@transient private val parameters: CaseInsensitiveMap)
  extends Serializable {

  import OrcOptions._

  def this(parameters: Map[String, String]) = this(new CaseInsensitiveMap(parameters))

  /**
   * Compression codec to use. By default snappy compression.
   * Acceptable values are defined in [[shortOrcCompressionCodecNames]].
   */
  val compressionCodec: String = {
    // `orc.compress` is a ORC configuration. So, here we respect this as an option but
    // `compression` has higher precedence than `orc.compress`. It means if both are set,
    // we will use `compression`.
    val orcCompressionConf = parameters.get(OrcRelation.ORC_COMPRESSION)
    val codecName = parameters
      .get("compression")
      .orElse(orcCompressionConf)
      .getOrElse("snappy").toLowerCase
    if (!shortOrcCompressionCodecNames.contains(codecName)) {
      val availableCodecs = shortOrcCompressionCodecNames.keys.map(_.toLowerCase)
      throw new IllegalArgumentException(s"Codec [$codecName] " +
        s"is not available. Available codecs are ${availableCodecs.mkString(", ")}.")
    }
    shortOrcCompressionCodecNames(codecName)
  }
}

private[orc] object OrcOptions {
  // The ORC compression short names
  private val shortOrcCompressionCodecNames = Map(
    "none" -> "NONE",
    "uncompressed" -> "NONE",
    "snappy" -> "SNAPPY",
    "zlib" -> "ZLIB",
    "lzo" -> "LZO")
}
