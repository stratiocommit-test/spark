/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.sources

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class CommitFailureTestRelationSuite extends SQLTestUtils with TestHiveSingleton {
  // When committing a task, `CommitFailureTestSource` throws an exception for testing purpose.
  val dataSourceName: String = classOf[CommitFailureTestSource].getCanonicalName

  test("SPARK-7684: commitTask() failure should fallback to abortTask()") {
    withTempPath { file =>
      // Here we coalesce partition number to 1 to ensure that only a single task is issued.  This
      // prevents race condition happened when FileOutputCommitter tries to remove the `_temporary`
      // directory while committing/aborting the job.  See SPARK-8513 for more details.
      val df = spark.range(0, 10).coalesce(1)
      intercept[SparkException] {
        df.write.format(dataSourceName).save(file.getCanonicalPath)
      }

      val fs = new Path(file.getCanonicalPath).getFileSystem(SparkHadoopUtil.get.conf)
      assert(!fs.exists(new Path(file.getCanonicalPath, "_temporary")))
    }
  }

  test("call failure callbacks before close writer - default") {
    SimpleTextRelation.failCommitter = false
    withTempPath { file =>
      // fail the job in the middle of writing
      val divideByZero = udf((x: Int) => { x / (x - 1)})
      val df = spark.range(0, 10).coalesce(1).select(divideByZero(col("id")))

      SimpleTextRelation.callbackCalled = false
      intercept[SparkException] {
        df.write.format(dataSourceName).save(file.getCanonicalPath)
      }
      assert(SimpleTextRelation.callbackCalled, "failure callback should be called")

      val fs = new Path(file.getCanonicalPath).getFileSystem(SparkHadoopUtil.get.conf)
      assert(!fs.exists(new Path(file.getCanonicalPath, "_temporary")))
    }
  }

  test("call failure callbacks before close writer - partitioned") {
    SimpleTextRelation.failCommitter = false
    withTempPath { file =>
      // fail the job in the middle of writing
      val df = spark.range(0, 10).coalesce(1).select(col("id").mod(2).as("key"), col("id"))

      SimpleTextRelation.callbackCalled = false
      SimpleTextRelation.failWriter = true
      intercept[SparkException] {
        df.write.format(dataSourceName).partitionBy("key").save(file.getCanonicalPath)
      }
      assert(SimpleTextRelation.callbackCalled, "failure callback should be called")

      val fs = new Path(file.getCanonicalPath).getFileSystem(SparkHadoopUtil.get.conf)
      assert(!fs.exists(new Path(file.getCanonicalPath, "_temporary")))
    }
  }
}
