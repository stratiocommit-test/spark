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

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, Time}

private[streaming]
class UnionDStream[T: ClassTag](parents: Array[DStream[T]])
  extends DStream[T](parents.head.ssc) {

  require(parents.length > 0, "List of DStreams to union is empty")
  require(parents.map(_.ssc).distinct.length == 1, "Some of the DStreams have different contexts")
  require(parents.map(_.slideDuration).distinct.length == 1,
    "Some of the DStreams have different slide durations")

  override def dependencies: List[DStream[_]] = parents.toList

  override def slideDuration: Duration = parents.head.slideDuration

  override def compute(validTime: Time): Option[RDD[T]] = {
    val rdds = new ArrayBuffer[RDD[T]]()
    parents.map(_.getOrCompute(validTime)).foreach {
      case Some(rdd) => rdds += rdd
      case None => throw new SparkException("Could not generate RDD from a parent for unifying at" +
        s" time $validTime")
    }
    if (rdds.nonEmpty) {
      Some(ssc.sc.union(rdds))
    } else {
      None
    }
  }
}
