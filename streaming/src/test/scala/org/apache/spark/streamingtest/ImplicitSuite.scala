/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streamingtest

/**
 * A test suite to make sure all `implicit` functions work correctly.
 *
 * As `implicit` is a compiler feature, we don't need to run this class.
 * What we need to do is making the compiler happy.
 */
class ImplicitSuite {

  // We only want to test if `implicit` works well with the compiler,
  // so we don't need a real DStream.
  def mockDStream[T]: org.apache.spark.streaming.dstream.DStream[T] = null

  def testToPairDStreamFunctions(): Unit = {
    val dstream: org.apache.spark.streaming.dstream.DStream[(Int, Int)] = mockDStream
    dstream.groupByKey()
  }
}
