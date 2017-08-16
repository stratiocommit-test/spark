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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * A source of continually arriving data for a streaming query. A [[Source]] must have a
 * monotonically increasing notion of progress that can be represented as an [[Offset]]. Spark
 * will regularly query each [[Source]] to see if any more data is available.
 */
trait Source  {

  /** Returns the schema of the data from this source */
  def schema: StructType

  /**
   * Returns the maximum available offset for this source.
   * Returns `None` if this source has never received any data.
   */
  def getOffset: Option[Offset]

  /**
   * Returns the data that is between the offsets (`start`, `end`]. When `start` is `None`,
   * then the batch should begin with the first record. This method must always return the
   * same data for a particular `start` and `end` pair; even after the Source has been restarted
   * on a different node.
   *
   * Higher layers will always call this method with a value of `start` greater than or equal
   * to the last value passed to `commit` and a value of `end` less than or equal to the
   * last value returned by `getOffset`
   *
   * It is possible for the [[Offset]] type to be a [[SerializedOffset]] when it was
   * obtained from the log. Moreover, [[StreamExecution]] only compares the [[Offset]]
   * JSON representation to determine if the two objects are equal. This could have
   * ramifications when upgrading [[Offset]] JSON formats i.e., two equivalent [[Offset]]
   * objects could differ between version. Consequently, [[StreamExecution]] may call
   * this method with two such equivalent [[Offset]] objects. In which case, the [[Source]]
   * should return an empty [[DataFrame]]
   */
  def getBatch(start: Option[Offset], end: Offset): DataFrame

  /**
   * Informs the source that Spark has completed processing all data for offsets less than or
   * equal to `end` and will only request offsets greater than `end` in the future.
   */
  def commit(end: Offset) : Unit = {}

  /** Stop this source and free any resources it has allocated. */
  def stop(): Unit
}
