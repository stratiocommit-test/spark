/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming

import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.spark.api.java.JavaRDDLike
import org.apache.spark.streaming.api.java.{JavaDStreamLike, JavaDStream, JavaStreamingContext}

/** Exposes streaming test functionality in a Java-friendly way. */
trait JavaTestBase extends TestSuiteBase {

  /**
   * Create a [[org.apache.spark.streaming.TestInputStream]] and attach it to the supplied context.
   * The stream will be derived from the supplied lists of Java objects.
   */
  def attachTestInputStream[T](
      ssc: JavaStreamingContext,
      data: JList[JList[T]],
      numPartitions: Int) = {
    val seqData = data.asScala.map(_.asScala)

    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    val dstream = new TestInputStream[T](ssc.ssc, seqData, numPartitions)
    new JavaDStream[T](dstream)
  }

  /**
   * Attach a provided stream to it's associated StreamingContext as a
   * [[org.apache.spark.streaming.TestOutputStream]].
   **/
  def attachTestOutputStream[T, This <: JavaDStreamLike[T, This, R], R <: JavaRDDLike[T, R]](
      dstream: JavaDStreamLike[T, This, R]) = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    val ostream = new TestOutputStreamWithPartitions(dstream.dstream)
    ostream.register()
  }

  /**
   * Process all registered streams for a numBatches batches, failing if
   * numExpectedOutput RDD's are not generated. Generated RDD's are collected
   * and returned, represented as a list for each batch interval.
   *
   * Returns a list of items for each RDD.
   */
  def runStreams[V](
      ssc: JavaStreamingContext, numBatches: Int, numExpectedOutput: Int): JList[JList[V]] = {
    implicit val cm: ClassTag[V] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[V]]
    ssc.getState()
    val res = runStreams[V](ssc.ssc, numBatches, numExpectedOutput)
    res.map(_.asJava).toSeq.asJava
  }

  /**
   * Process all registered streams for a numBatches batches, failing if
   * numExpectedOutput RDD's are not generated. Generated RDD's are collected
   * and returned, represented as a list for each batch interval.
   *
   * Returns a sequence of RDD's. Each RDD is represented as several sequences of items, each
   * representing one partition.
   */
  def runStreamsWithPartitions[V](ssc: JavaStreamingContext, numBatches: Int,
      numExpectedOutput: Int): JList[JList[JList[V]]] = {
    implicit val cm: ClassTag[V] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[V]]
    val res = runStreamsWithPartitions[V](ssc.ssc, numBatches, numExpectedOutput)
    res.map(entry => entry.map(_.asJava).asJava).toSeq.asJava
  }
}

object JavaTestUtils extends JavaTestBase {
  override def maxWaitTimeMillis = 20000

}

object JavaCheckpointTestUtils extends JavaTestBase {
  override def actuallyWait = true
}
