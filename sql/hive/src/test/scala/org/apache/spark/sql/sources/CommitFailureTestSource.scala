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

import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.TaskContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

class CommitFailureTestSource extends SimpleTextSource {
  /**
   * Prepares a write job and returns an
   * [[org.apache.spark.sql.execution.datasources.OutputWriterFactory]].
   * Client side job preparation can
   * be put here.  For example, user defined output committer can be configured here
   * by setting the output committer class in the conf of spark.sql.sources.outputCommitterClass.
   */
  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory =
    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new SimpleTextOutputWriter(path, context) {
          var failed = false
          TaskContext.get().addTaskFailureListener { (t: TaskContext, e: Throwable) =>
            failed = true
            SimpleTextRelation.callbackCalled = true
          }

          override def write(row: Row): Unit = {
            if (SimpleTextRelation.failWriter) {
              sys.error("Intentional task writer failure for testing purpose.")

            }
            super.write(row)
          }

          override def close(): Unit = {
            super.close()
            sys.error("Intentional task commitment failure for testing purpose.")
          }
        }
      }

      override def getFileExtension(context: TaskAttemptContext): String = ""
    }

  override def shortName(): String = "commit-failure-test"
}
