/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.sparktest

/**
 * A test suite to make sure all `implicit` functions work correctly.
 * Please don't `import org.apache.spark.SparkContext._` in this class.
 *
 * As `implicit` is a compiler feature, we don't need to run this class.
 * What we need to do is making the compiler happy.
 */
class ImplicitSuite {

  // We only want to test if `implicit` works well with the compiler, so we don't need a real
  // SparkContext.
  def mockSparkContext[T]: org.apache.spark.SparkContext = null

  // We only want to test if `implicit` works well with the compiler, so we don't need a real RDD.
  def mockRDD[T]: org.apache.spark.rdd.RDD[T] = null

  def testRddToPairRDDFunctions(): Unit = {
    val rdd: org.apache.spark.rdd.RDD[(Int, Int)] = mockRDD
    rdd.groupByKey()
  }

  def testRddToAsyncRDDActions(): Unit = {
    val rdd: org.apache.spark.rdd.RDD[Int] = mockRDD
    rdd.countAsync()
  }

  def testRddToSequenceFileRDDFunctions(): Unit = {
    val rdd: org.apache.spark.rdd.RDD[(Int, Int)] = mockRDD
    rdd.saveAsSequenceFile("/a/test/path")
  }

  def testRddToSequenceFileRDDFunctionsWithWritable(): Unit = {
    val rdd: org.apache.spark.rdd.RDD[(org.apache.hadoop.io.IntWritable, org.apache.hadoop.io.Text)]
      = mockRDD
    rdd.saveAsSequenceFile("/a/test/path")
  }

  def testRddToSequenceFileRDDFunctionsWithBytesArray(): Unit = {
    val rdd: org.apache.spark.rdd.RDD[(Int, Array[Byte])] = mockRDD
    rdd.saveAsSequenceFile("/a/test/path")
  }

  def testRddToOrderedRDDFunctions(): Unit = {
    val rdd: org.apache.spark.rdd.RDD[(Int, Int)] = mockRDD
    rdd.sortByKey()
  }

  def testDoubleRDDToDoubleRDDFunctions(): Unit = {
    val rdd: org.apache.spark.rdd.RDD[Double] = mockRDD
    rdd.stats()
  }

  def testNumericRDDToDoubleRDDFunctions(): Unit = {
    val rdd: org.apache.spark.rdd.RDD[Int] = mockRDD
    rdd.stats()
  }

  def testDoubleAccumulatorParam(): Unit = {
    val sc = mockSparkContext
    sc.accumulator(123.4)
  }

  def testIntAccumulatorParam(): Unit = {
    val sc = mockSparkContext
    sc.accumulator(123)
  }

  def testLongAccumulatorParam(): Unit = {
    val sc = mockSparkContext
    sc.accumulator(123L)
  }

  def testFloatAccumulatorParam(): Unit = {
    val sc = mockSparkContext
    sc.accumulator(123F)
  }

  def testIntWritableConverter(): Unit = {
    val sc = mockSparkContext
    sc.sequenceFile[Int, Int]("/a/test/path")
  }

  def testLongWritableConverter(): Unit = {
    val sc = mockSparkContext
    sc.sequenceFile[Long, Long]("/a/test/path")
  }

  def testDoubleWritableConverter(): Unit = {
    val sc = mockSparkContext
    sc.sequenceFile[Double, Double]("/a/test/path")
  }

  def testFloatWritableConverter(): Unit = {
    val sc = mockSparkContext
    sc.sequenceFile[Float, Float]("/a/test/path")
  }

  def testBooleanWritableConverter(): Unit = {
    val sc = mockSparkContext
    sc.sequenceFile[Boolean, Boolean]("/a/test/path")
  }

  def testBytesWritableConverter(): Unit = {
    val sc = mockSparkContext
    sc.sequenceFile[Array[Byte], Array[Byte]]("/a/test/path")
  }

  def testStringWritableConverter(): Unit = {
    val sc = mockSparkContext
    sc.sequenceFile[String, String]("/a/test/path")
  }

  def testWritableWritableConverter(): Unit = {
    val sc = mockSparkContext
    sc.sequenceFile[org.apache.hadoop.io.Text, org.apache.hadoop.io.Text]("/a/test/path")
  }
}
