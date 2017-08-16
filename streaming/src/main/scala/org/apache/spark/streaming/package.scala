/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark

/**
 * Spark Streaming functionality. [[org.apache.spark.streaming.StreamingContext]] serves as the main
 * entry point to Spark Streaming, while [[org.apache.spark.streaming.dstream.DStream]] is the data
 * type representing a continuous sequence of RDDs, representing a continuous stream of data.
 *
 * In addition, [[org.apache.spark.streaming.dstream.PairDStreamFunctions]] contains operations
 * available only on DStreams
 * of key-value pairs, such as `groupByKey` and `reduceByKey`. These operations are automatically
 * available on any DStream of the right type (e.g. DStream[(Int, Int)] through implicit
 * conversions.
 *
 * For the Java API of Spark Streaming, take a look at the
 * [[org.apache.spark.streaming.api.java.JavaStreamingContext]] which serves as the entry point, and
 * the [[org.apache.spark.streaming.api.java.JavaDStream]] and the
 * [[org.apache.spark.streaming.api.java.JavaPairDStream]] which have the DStream functionality.
 */
package object streaming {
  // For package docs only
}
