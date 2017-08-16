/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.dstream

import scala.reflect.ClassTag

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, Time}

private[streaming]
class StateDStream[K: ClassTag, V: ClassTag, S: ClassTag](
    parent: DStream[(K, V)],
    updateFunc: (Time, Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
    partitioner: Partitioner,
    preservePartitioning: Boolean,
    initialRDD: Option[RDD[(K, S)]]
  ) extends DStream[(K, S)](parent.ssc) {

  super.persist(StorageLevel.MEMORY_ONLY_SER)

  override def dependencies: List[DStream[_]] = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override val mustCheckpoint = true

  private [this] def computeUsingPreviousRDD(
      batchTime: Time,
      parentRDD: RDD[(K, V)],
      prevStateRDD: RDD[(K, S)]) = {
    // Define the function for the mapPartition operation on cogrouped RDD;
    // first map the cogrouped tuple to tuples of required type,
    // and then apply the update function
    val updateFuncLocal = updateFunc
    val finalFunc = (iterator: Iterator[(K, (Iterable[V], Iterable[S]))]) => {
      val i = iterator.map { t =>
        val itr = t._2._2.iterator
        val headOption = if (itr.hasNext) Some(itr.next()) else None
        (t._1, t._2._1.toSeq, headOption)
      }
      updateFuncLocal(batchTime, i)
    }
    val cogroupedRDD = parentRDD.cogroup(prevStateRDD, partitioner)
    val stateRDD = cogroupedRDD.mapPartitions(finalFunc, preservePartitioning)
    Some(stateRDD)
  }

  override def compute(validTime: Time): Option[RDD[(K, S)]] = {

    // Try to get the previous state RDD
    getOrCompute(validTime - slideDuration) match {

      case Some(prevStateRDD) =>    // If previous state RDD exists
        // Try to get the parent RDD
        parent.getOrCompute(validTime) match {
          case Some(parentRDD) =>    // If parent RDD exists, then compute as usual
            computeUsingPreviousRDD (validTime, parentRDD, prevStateRDD)
          case None =>     // If parent RDD does not exist
            // Re-apply the update function to the old state RDD
            val updateFuncLocal = updateFunc
            val finalFunc = (iterator: Iterator[(K, S)]) => {
              val i = iterator.map(t => (t._1, Seq[V](), Option(t._2)))
              updateFuncLocal(validTime, i)
            }
            val stateRDD = prevStateRDD.mapPartitions(finalFunc, preservePartitioning)
            Some(stateRDD)
        }

      case None =>    // If previous session RDD does not exist (first input data)
        // Try to get the parent RDD
        parent.getOrCompute(validTime) match {
          case Some(parentRDD) =>   // If parent RDD exists, then compute as usual
            initialRDD match {
              case None =>
                // Define the function for the mapPartition operation on grouped RDD;
                // first map the grouped tuple to tuples of required type,
                // and then apply the update function
                val updateFuncLocal = updateFunc
                val finalFunc = (iterator: Iterator[(K, Iterable[V])]) => {
                  updateFuncLocal (validTime,
                    iterator.map (tuple => (tuple._1, tuple._2.toSeq, None)))
                }

                val groupedRDD = parentRDD.groupByKey(partitioner)
                val sessionRDD = groupedRDD.mapPartitions(finalFunc, preservePartitioning)
                // logDebug("Generating state RDD for time " + validTime + " (first)")
                Some (sessionRDD)
              case Some (initialStateRDD) =>
                computeUsingPreviousRDD(validTime, parentRDD, initialStateRDD)
            }
          case None => // If parent RDD does not exist, then nothing to do!
            // logDebug("Not generating state RDD (no previous state, no parent)")
            None
        }
    }
  }
}
