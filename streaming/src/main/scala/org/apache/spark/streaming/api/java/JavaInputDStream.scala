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

import org.apache.spark.streaming.dstream.InputDStream

/**
 * A Java-friendly interface to [[org.apache.spark.streaming.dstream.InputDStream]].
 */
class JavaInputDStream[T](val inputDStream: InputDStream[T])
  (implicit override val classTag: ClassTag[T]) extends JavaDStream[T](inputDStream) {
}

object JavaInputDStream {
  /**
   * Convert a scala [[org.apache.spark.streaming.dstream.InputDStream]] to a Java-friendly
   * [[org.apache.spark.streaming.api.java.JavaInputDStream]].
   */
  implicit def fromInputDStream[T: ClassTag](
      inputDStream: InputDStream[T]): JavaInputDStream[T] = {
    new JavaInputDStream[T](inputDStream)
  }
}
