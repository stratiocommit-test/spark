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

/**
 * An offset is a monotonically increasing metric used to track progress in the computation of a
 * stream. Since offsets are retrieved from a [[Source]] by a single thread, we know the global
 * ordering of two [[Offset]] instances.  We do assume that if two offsets are `equal` then no
 * new data has arrived.
 */
abstract class Offset {

  /**
   * Equality based on JSON string representation. We leverage the
   * JSON representation for normalization between the Offset's
   * in memory and on disk representations.
   */
  override def equals(obj: Any): Boolean = obj match {
    case o: Offset => this.json == o.json
    case _ => false
  }

  override def hashCode(): Int = this.json.hashCode

  override def toString(): String = this.json.toString

  /**
   * A JSON-serialized representation of an Offset that is
   * used for saving offsets to the offset log.
   * Note: We assume that equivalent/equal offsets serialize to
   * identical JSON strings.
   *
   * @return JSON string encoding
   */
  def json: String
}

/**
 * Used when loading a JSON serialized offset from external storage.
 * We are currently not responsible for converting JSON serialized
 * data into an internal (i.e., object) representation. Sources should
 * define a factory method in their source Offset companion objects
 * that accepts a [[SerializedOffset]] for doing the conversion.
 */
case class SerializedOffset(override val json: String) extends Offset
