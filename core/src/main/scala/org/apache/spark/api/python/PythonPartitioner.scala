/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.api.python

import org.apache.spark.Partitioner
import org.apache.spark.util.Utils

/**
 * A [[org.apache.spark.Partitioner]] that performs handling of long-valued keys, for use by the
 * Python API.
 *
 * Stores the unique id() of the Python-side partitioning function so that it is incorporated into
 * equality comparisons.  Correctness requires that the id is a unique identifier for the
 * lifetime of the program (i.e. that it is not re-used as the id of a different partitioning
 * function).  This can be ensured by using the Python id() function and maintaining a reference
 * to the Python partitioning function so that its id() is not reused.
 */

private[spark] class PythonPartitioner(
  override val numPartitions: Int,
  val pyPartitionFunctionId: Long)
  extends Partitioner {

  override def getPartition(key: Any): Int = key match {
    case null => 0
    // we don't trust the Python partition function to return valid partition ID's so
    // let's do a modulo numPartitions in any case
    case key: Long => Utils.nonNegativeMod(key.toInt, numPartitions)
    case _ => Utils.nonNegativeMod(key.hashCode(), numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: PythonPartitioner =>
      h.numPartitions == numPartitions && h.pyPartitionFunctionId == pyPartitionFunctionId
    case _ =>
      false
  }

  override def hashCode: Int = 31 * numPartitions + pyPartitionFunctionId.hashCode
}
