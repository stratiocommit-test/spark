/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.datasources

import java.io.{File, FilenameFilter}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext

class HadoopFsRelationSuite extends QueryTest with SharedSQLContext {

  test("sizeInBytes should be the total size of all files") {
    withTempDir{ dir =>
      dir.delete()
      spark.range(1000).write.parquet(dir.toString)
      // ignore hidden files
      val allFiles = dir.listFiles(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = {
          !name.startsWith(".")
        }
      })
      val totalSize = allFiles.map(_.length()).sum
      val df = spark.read.parquet(dir.toString)
      assert(df.queryExecution.logical.statistics.sizeInBytes === BigInt(totalSize))
    }
  }
}
