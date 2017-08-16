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

import scala.collection.{immutable, GenTraversableOnce}

/**
 * A helper class that looks like a Map[Source, Offset].
 */
class StreamProgress(
    val baseMap: immutable.Map[Source, Offset] = new immutable.HashMap[Source, Offset])
  extends scala.collection.immutable.Map[Source, Offset] {

  def toOffsetSeq(source: Seq[Source], metadata: OffsetSeqMetadata): OffsetSeq = {
    OffsetSeq(source.map(get), Some(metadata))
  }

  override def toString: String =
    baseMap.map { case (k, v) => s"$k: $v"}.mkString("{", ",", "}")

  override def +[B1 >: Offset](kv: (Source, B1)): Map[Source, B1] = baseMap + kv

  override def get(key: Source): Option[Offset] = baseMap.get(key)

  override def iterator: Iterator[(Source, Offset)] = baseMap.iterator

  override def -(key: Source): Map[Source, Offset] = baseMap - key

  def ++(updates: GenTraversableOnce[(Source, Offset)]): StreamProgress = {
    new StreamProgress(baseMap ++ updates)
  }
}
