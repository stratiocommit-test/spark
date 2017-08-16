/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.api.java

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
 * A Java-friendly interface to [[org.apache.spark.streaming.dstream.ReceiverInputDStream]], the
 * abstract class for defining any input stream that receives data over the network.
 */
class JavaPairReceiverInputDStream[K, V](val receiverInputDStream: ReceiverInputDStream[(K, V)])
    (implicit override val kClassTag: ClassTag[K], override implicit val vClassTag: ClassTag[V])
  extends JavaPairInputDStream[K, V](receiverInputDStream) {
}

object JavaPairReceiverInputDStream {
  /**
   * Convert a scala [[org.apache.spark.streaming.dstream.ReceiverInputDStream]] to a Java-friendly
   * [[org.apache.spark.streaming.api.java.JavaReceiverInputDStream]].
   */
  implicit def fromReceiverInputDStream[K: ClassTag, V: ClassTag](
      receiverInputDStream: ReceiverInputDStream[(K, V)]): JavaPairReceiverInputDStream[K, V] = {
    new JavaPairReceiverInputDStream[K, V](receiverInputDStream)
  }
}
