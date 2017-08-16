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

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, Time}

private[streaming]
class TransformedDStream[U: ClassTag] (
    parents: Seq[DStream[_]],
    transformFunc: (Seq[RDD[_]], Time) => RDD[U]
  ) extends DStream[U](parents.head.ssc) {

  require(parents.nonEmpty, "List of DStreams to transform is empty")
  require(parents.map(_.ssc).distinct.size == 1, "Some of the DStreams have different contexts")
  require(parents.map(_.slideDuration).distinct.size == 1,
    "Some of the DStreams have different slide durations")

  override def dependencies: List[DStream[_]] = parents.toList

  override def slideDuration: Duration = parents.head.slideDuration

  override def compute(validTime: Time): Option[RDD[U]] = {
    val parentRDDs = parents.map { parent => parent.getOrCompute(validTime).getOrElse(
      // Guard out against parent DStream that return None instead of Some(rdd) to avoid NPE
      throw new SparkException(s"Couldn't generate RDD from parent at time $validTime"))
    }
    val transformedRDD = transformFunc(parentRDDs, validTime)
    if (transformedRDD == null) {
      throw new SparkException("Transform function must not return null. " +
        "Return SparkContext.emptyRDD() instead to represent no element " +
        "as the result of transformation.")
    }
    Some(transformedRDD)
  }

  /**
   * Wrap a body of code such that the call site and operation scope
   * information are passed to the RDDs created in this body properly.
   * This has been overridden to make sure that `displayInnerRDDOps` is always `true`, that is,
   * the inner scopes and callsites of RDDs generated in `DStream.transform` are always
   * displayed in the UI.
   */
  override protected[streaming] def createRDDWithLocalProperties[U](
      time: Time,
      displayInnerRDDOps: Boolean)(body: => U): U = {
    super.createRDDWithLocalProperties(time, displayInnerRDDOps = true)(body)
  }
}
