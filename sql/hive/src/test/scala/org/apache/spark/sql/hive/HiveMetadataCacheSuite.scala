/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.hive

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

/**
 * Test suite to handle metadata cache related.
 */
class HiveMetadataCacheSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  test("SPARK-16337 temporary view refresh") {
    withTempView("view_refresh") {
      withTable("view_table") {
        // Create a Parquet directory
        spark.range(start = 0, end = 100, step = 1, numPartitions = 3)
          .write.saveAsTable("view_table")

        // Read the table in
        spark.table("view_table").filter("id > -1").createOrReplaceTempView("view_refresh")
        assert(sql("select count(*) from view_refresh").first().getLong(0) == 100)

        // Delete a file using the Hadoop file system interface since the path returned by
        // inputFiles is not recognizable by Java IO.
        val p = new Path(spark.table("view_table").inputFiles.head)
        assert(p.getFileSystem(hiveContext.sessionState.newHadoopConf()).delete(p, false))

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
      }
    }
  }

  def testCaching(pruningEnabled: Boolean): Unit = {
    test(s"partitioned table is cached when partition pruning is $pruningEnabled") {
      withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> pruningEnabled.toString) {
        withTable("test") {
          withTempDir { dir =>
            spark.range(5).selectExpr("id", "id as f1", "id as f2").write
              .partitionBy("f1", "f2")
              .mode("overwrite")
              .parquet(dir.getAbsolutePath)

            spark.sql(s"""
              |create external table test (id long)
              |partitioned by (f1 int, f2 int)
              |stored as parquet
              |location "${dir.getAbsolutePath}"""".stripMargin)
            spark.sql("msck repair table test")

            val df = spark.sql("select * from test")
            assert(sql("select * from test").count() == 5)

            def deleteRandomFile(): Unit = {
              val p = new Path(spark.table("test").inputFiles.head)
              assert(p.getFileSystem(hiveContext.sessionState.newHadoopConf()).delete(p, true))
            }

            // Delete a file, then assert that we tried to read it. This means the table was cached.
            deleteRandomFile()
            val e = intercept[SparkException] {
              sql("select * from test").count()
            }
            assert(e.getMessage.contains("FileNotFoundException"))

            // Test refreshing the cache.
            spark.catalog.refreshTable("test")
            assert(sql("select * from test").count() == 4)
            assert(spark.table("test").inputFiles.length == 4)

            // Test refresh by path separately since it goes through different code paths than
            // refreshTable does.
            deleteRandomFile()
            spark.catalog.cacheTable("test")
            spark.catalog.refreshByPath("/some-invalid-path")  // no-op
            val e2 = intercept[SparkException] {
              sql("select * from test").count()
            }
            assert(e2.getMessage.contains("FileNotFoundException"))
            spark.catalog.refreshByPath(dir.getAbsolutePath)
            assert(sql("select * from test").count() == 3)
          }
        }
      }
    }
  }

  for (pruningEnabled <- Seq(true, false)) {
    testCaching(pruningEnabled)
  }
}
