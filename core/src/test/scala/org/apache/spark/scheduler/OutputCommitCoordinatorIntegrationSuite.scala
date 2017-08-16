/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.scheduler

import org.apache.hadoop.mapred.{FileOutputCommitter, TaskAttemptContext}
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite, TaskContext}
import org.apache.spark.util.Utils

/**
 * Integration tests for the OutputCommitCoordinator.
 *
 * See also: [[OutputCommitCoordinatorSuite]] for unit tests that use mocks.
 */
class OutputCommitCoordinatorIntegrationSuite
  extends SparkFunSuite
  with LocalSparkContext
  with Timeouts {

  override def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new SparkConf()
      .set("spark.hadoop.outputCommitCoordination.enabled", "true")
      .set("spark.hadoop.mapred.output.committer.class",
        classOf[ThrowExceptionOnFirstAttemptOutputCommitter].getCanonicalName)
    sc = new SparkContext("local[2, 4]", "test", conf)
  }

  test("exception thrown in OutputCommitter.commitTask()") {
    // Regression test for SPARK-10381
    failAfter(Span(60, Seconds)) {
      val tempDir = Utils.createTempDir()
      try {
        sc.parallelize(1 to 4, 2).map(_.toString).saveAsTextFile(tempDir.getAbsolutePath + "/out")
      } finally {
        Utils.deleteRecursively(tempDir)
      }
    }
  }
}

private class ThrowExceptionOnFirstAttemptOutputCommitter extends FileOutputCommitter {
  override def commitTask(context: TaskAttemptContext): Unit = {
    val ctx = TaskContext.get()
    if (ctx.attemptNumber < 1) {
      throw new java.io.FileNotFoundException("Intentional exception")
    }
    super.commitTask(context)
  }
}
