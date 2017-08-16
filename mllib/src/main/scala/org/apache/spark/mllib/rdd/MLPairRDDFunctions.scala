/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.rdd

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.util.BoundedPriorityQueue

/**
 * :: DeveloperApi ::
 * Machine learning specific Pair RDD functions.
 */
@DeveloperApi
class MLPairRDDFunctions[K: ClassTag, V: ClassTag](self: RDD[(K, V)]) extends Serializable {
  /**
   * Returns the top k (largest) elements for each key from this RDD as defined by the specified
   * implicit Ordering[T].
   * If the number of elements for a certain key is less than k, all of them will be returned.
   *
   * @param num k, the number of top elements to return
   * @param ord the implicit ordering for T
   * @return an RDD that contains the top k values for each key
   */
  def topByKey(num: Int)(implicit ord: Ordering[V]): RDD[(K, Array[V])] = {
    self.aggregateByKey(new BoundedPriorityQueue[V](num)(ord))(
      seqOp = (queue, item) => {
        queue += item
      },
      combOp = (queue1, queue2) => {
        queue1 ++= queue2
      }
    ).mapValues(_.toArray.sorted(ord.reverse))  // This is a min-heap, so we reverse the order.
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
object MLPairRDDFunctions {
  /** Implicit conversion from a pair RDD to MLPairRDDFunctions. */
  implicit def fromPairRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): MLPairRDDFunctions[K, V] =
    new MLPairRDDFunctions[K, V](rdd)
}
