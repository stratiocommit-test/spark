/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.sql.SparkSession

class SQLExecutionSuite extends SparkFunSuite {

  test("concurrent query execution (SPARK-10548)") {
    // Try to reproduce the issue with the old SparkContext
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test")
    val badSparkContext = new BadSparkContext(conf)
    try {
      testConcurrentQueryExecution(badSparkContext)
      fail("unable to reproduce SPARK-10548")
    } catch {
      case e: IllegalArgumentException =>
        assert(e.getMessage.contains(SQLExecution.EXECUTION_ID_KEY))
    } finally {
      badSparkContext.stop()
    }

    // Verify that the issue is fixed with the latest SparkContext
    val goodSparkContext = new SparkContext(conf)
    try {
      testConcurrentQueryExecution(goodSparkContext)
    } finally {
      goodSparkContext.stop()
    }
  }

  test("concurrent query execution with fork-join pool (SPARK-13747)") {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("test")
      .getOrCreate()

    import spark.implicits._
    try {
      // Should not throw IllegalArgumentException
      (1 to 100).par.foreach { _ =>
        spark.sparkContext.parallelize(1 to 5).map { i => (i, i) }.toDF("a", "b").count()
      }
    } finally {
      spark.sparkContext.stop()
    }
  }

  /**
   * Trigger SPARK-10548 by mocking a parent and its child thread executing queries concurrently.
   */
  private def testConcurrentQueryExecution(sc: SparkContext): Unit = {
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    // Initialize local properties. This is necessary for the test to pass.
    sc.getLocalProperties

    // Set up a thread that runs executes a simple SQL query.
    // Before starting the thread, mutate the execution ID in the parent.
    // The child thread should not see the effect of this change.
    var throwable: Option[Throwable] = None
    val child = new Thread {
      override def run(): Unit = {
        try {
          sc.parallelize(1 to 100).map { i => (i, i) }.toDF("a", "b").collect()
        } catch {
          case t: Throwable =>
            throwable = Some(t)
        }

      }
    }
    sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, "anything")
    child.start()
    child.join()

    // The throwable is thrown from the child thread so it doesn't have a helpful stack trace
    throwable.foreach { t =>
      t.setStackTrace(t.getStackTrace ++ Thread.currentThread.getStackTrace)
      throw t
    }
  }

}

/**
 * A bad [[SparkContext]] that does not clone the inheritable thread local properties
 * when passing them to children threads.
 */
private class BadSparkContext(conf: SparkConf) extends SparkContext(conf) {
  protected[spark] override val localProperties = new InheritableThreadLocal[Properties] {
    override protected def childValue(parent: Properties): Properties = new Properties(parent)
    override protected def initialValue(): Properties = new Properties()
  }
}
