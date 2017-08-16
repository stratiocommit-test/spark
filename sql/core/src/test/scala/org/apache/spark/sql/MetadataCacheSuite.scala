/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql

import java.io.File

import org.apache.spark.SparkException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

/**
 * Test suite to handle metadata cache related.
 */
class MetadataCacheSuite extends QueryTest with SharedSQLContext {

  /** Removes one data file in the given directory. */
  private def deleteOneFileInDirectory(dir: File): Unit = {
    assert(dir.isDirectory)
    val oneFile = dir.listFiles().find { file =>
      !file.getName.startsWith("_") && !file.getName.startsWith(".")
    }
    assert(oneFile.isDefined)
    oneFile.foreach(_.delete())
  }

  test("SPARK-16336 Suggest doing table refresh when encountering FileNotFoundException") {
    withTempPath { (location: File) =>
      // Create a Parquet directory
      spark.range(start = 0, end = 100, step = 1, numPartitions = 3)
        .write.parquet(location.getAbsolutePath)

      // Read the directory in
      val df = spark.read.parquet(location.getAbsolutePath)
      assert(df.count() == 100)

      // Delete a file
      deleteOneFileInDirectory(location)

      // Read it again and now we should see a FileNotFoundException
      val e = intercept[SparkException] {
        df.count()
      }
      assert(e.getMessage.contains("FileNotFoundException"))
      assert(e.getMessage.contains("REFRESH"))
    }
  }

  test("SPARK-16337 temporary view refresh") {
    withTempView("view_refresh") { withTempPath { (location: File) =>
      // Create a Parquet directory
      spark.range(start = 0, end = 100, step = 1, numPartitions = 3)
        .write.parquet(location.getAbsolutePath)

      // Read the directory in
      spark.read.parquet(location.getAbsolutePath).createOrReplaceTempView("view_refresh")
      assert(sql("select count(*) from view_refresh").first().getLong(0) == 100)

      // Delete a file
      deleteOneFileInDirectory(location)

      // Read it again and now we should see a FileNotFoundException
      val e = intercept[SparkException] {
        sql("select count(*) from view_refresh").first()
      }
      assert(e.getMessage.contains("FileNotFoundException"))
      assert(e.getMessage.contains("REFRESH"))

      // Refresh and we should be able to read it again.
      spark.catalog.refreshTable("view_refresh")
      val newCount = sql("select count(*) from view_refresh").first().getLong(0)
      assert(newCount > 0 && newCount < 100)
    }}
  }

  test("case sensitivity support in temporary view refresh") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempView("view_refresh") {
        withTempPath { (location: File) =>
          // Create a Parquet directory
          spark.range(start = 0, end = 100, step = 1, numPartitions = 3)
            .write.parquet(location.getAbsolutePath)

          // Read the directory in
          spark.read.parquet(location.getAbsolutePath).createOrReplaceTempView("view_refresh")

          // Delete a file
          deleteOneFileInDirectory(location)
          intercept[SparkException](sql("select count(*) from view_refresh").first())

          // Refresh and we should be able to read it again.
          spark.catalog.refreshTable("vIeW_reFrEsH")
          val newCount = sql("select count(*) from view_refresh").first().getLong(0)
          assert(newCount > 0 && newCount < 100)
        }
      }
    }
  }
}
