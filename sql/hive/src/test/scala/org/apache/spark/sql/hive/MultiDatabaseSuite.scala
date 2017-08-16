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

import org.apache.spark.sql.{AnalysisException, QueryTest, SaveMode}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class MultiDatabaseSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  private lazy val df = spark.range(10).coalesce(1).toDF()

  private def checkTablePath(dbName: String, tableName: String): Unit = {
    val metastoreTable = spark.sharedState.externalCatalog.getTable(dbName, tableName)
    val expectedPath =
      spark.sharedState.externalCatalog.getDatabase(dbName).locationUri + "/" + tableName

    assert(metastoreTable.location === expectedPath)
  }

  private def getTableNames(dbName: Option[String] = None): Array[String] = {
    dbName match {
      case Some(db) => spark.catalog.listTables(db).collect().map(_.name)
      case None => spark.catalog.listTables().collect().map(_.name)
    }
  }

  test(s"saveAsTable() to non-default database - with USE - Overwrite") {
    withTempDatabase { db =>
      activateDatabase(db) {
        df.write.mode(SaveMode.Overwrite).saveAsTable("t")
        assert(getTableNames().contains("t"))
        checkAnswer(spark.table("t"), df)
      }

      assert(getTableNames(Option(db)).contains("t"))
      checkAnswer(spark.table(s"$db.t"), df)

      checkTablePath(db, "t")
    }
  }

  test(s"saveAsTable() to non-default database - without USE - Overwrite") {
    withTempDatabase { db =>
      df.write.mode(SaveMode.Overwrite).saveAsTable(s"$db.t")
      assert(getTableNames(Option(db)).contains("t"))
      checkAnswer(spark.table(s"$db.t"), df)

      checkTablePath(db, "t")
    }
  }

  test(s"createExternalTable() to non-default database - with USE") {
    withTempDatabase { db =>
      activateDatabase(db) {
        withTempPath { dir =>
          val path = dir.getCanonicalPath
          df.write.format("parquet").mode(SaveMode.Overwrite).save(path)

          spark.catalog.createExternalTable("t", path, "parquet")
          assert(getTableNames(Option(db)).contains("t"))
          checkAnswer(spark.table("t"), df)

          sql(
            s"""
              |CREATE TABLE t1
              |USING parquet
              |OPTIONS (
              |  path '$path'
              |)
            """.stripMargin)
          assert(getTableNames(Option(db)).contains("t1"))
          checkAnswer(spark.table("t1"), df)
        }
      }
    }
  }

  test(s"createExternalTable() to non-default database - without USE") {
    withTempDatabase { db =>
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        df.write.format("parquet").mode(SaveMode.Overwrite).save(path)
        spark.catalog.createExternalTable(s"$db.t", path, "parquet")

        assert(getTableNames(Option(db)).contains("t"))
        checkAnswer(spark.table(s"$db.t"), df)

        sql(
          s"""
              |CREATE TABLE $db.t1
              |USING parquet
              |OPTIONS (
              |  path '$path'
              |)
            """.stripMargin)
        assert(getTableNames(Option(db)).contains("t1"))
        checkAnswer(spark.table(s"$db.t1"), df)
      }
    }
  }

  test(s"saveAsTable() to non-default database - with USE - Append") {
    withTempDatabase { db =>
      activateDatabase(db) {
        df.write.mode(SaveMode.Overwrite).saveAsTable("t")
        df.write.mode(SaveMode.Append).saveAsTable("t")
        assert(getTableNames().contains("t"))
        checkAnswer(spark.table("t"), df.union(df))
      }

      assert(getTableNames(Option(db)).contains("t"))
      checkAnswer(spark.table(s"$db.t"), df.union(df))

      checkTablePath(db, "t")
    }
  }

  test(s"saveAsTable() to non-default database - without USE - Append") {
    withTempDatabase { db =>
      df.write.mode(SaveMode.Overwrite).saveAsTable(s"$db.t")
      df.write.mode(SaveMode.Append).saveAsTable(s"$db.t")
      assert(getTableNames(Option(db)).contains("t"))
      checkAnswer(spark.table(s"$db.t"), df.union(df))

      checkTablePath(db, "t")
    }
  }

  test(s"insertInto() non-default database - with USE") {
    withTempDatabase { db =>
      activateDatabase(db) {
        df.write.mode(SaveMode.Overwrite).saveAsTable("t")
        assert(getTableNames().contains("t"))

        df.write.insertInto(s"$db.t")
        checkAnswer(spark.table(s"$db.t"), df.union(df))
      }
    }
  }

  test(s"insertInto() non-default database - without USE") {
    withTempDatabase { db =>
      activateDatabase(db) {
        df.write.mode(SaveMode.Overwrite).saveAsTable("t")
        assert(getTableNames().contains("t"))
      }

      assert(getTableNames(Option(db)).contains("t"))

      df.write.insertInto(s"$db.t")
      checkAnswer(spark.table(s"$db.t"), df.union(df))
    }
  }

  test("Looks up tables in non-default database") {
    withTempDatabase { db =>
      activateDatabase(db) {
        sql("CREATE TABLE t (key INT)")
        checkAnswer(spark.table("t"), spark.emptyDataFrame)
      }

      checkAnswer(spark.table(s"$db.t"), spark.emptyDataFrame)
    }
  }

  test("Drops a table in a non-default database") {
    withTempDatabase { db =>
      activateDatabase(db) {
        sql(s"CREATE TABLE t (key INT)")
        assert(getTableNames().contains("t"))
        assert(!getTableNames(Option("default")).contains("t"))
      }

      assert(!getTableNames().contains("t"))
      assert(getTableNames(Option(db)).contains("t"))

      activateDatabase(db) {
        sql(s"DROP TABLE t")
        assert(!getTableNames().contains("t"))
        assert(!getTableNames(Option("default")).contains("t"))
      }

      assert(!getTableNames().contains("t"))
      assert(!getTableNames(Option(db)).contains("t"))
    }
  }

  test("Refreshes a table in a non-default database - with USE") {
    import org.apache.spark.sql.functions.lit

    withTempDatabase { db =>
      withTempPath { dir =>
        val path = dir.getCanonicalPath

        activateDatabase(db) {
          sql(
            s"""CREATE EXTERNAL TABLE t (id BIGINT)
               |PARTITIONED BY (p INT)
               |STORED AS PARQUET
               |LOCATION '$path'
             """.stripMargin)

          checkAnswer(spark.table("t"), spark.emptyDataFrame)

          df.write.parquet(s"$path/p=1")
          sql("ALTER TABLE t ADD PARTITION (p=1)")
          sql("REFRESH TABLE t")
          checkAnswer(spark.table("t"), df.withColumn("p", lit(1)))

          df.write.parquet(s"$path/p=2")
          sql("ALTER TABLE t ADD PARTITION (p=2)")
          spark.catalog.refreshTable("t")
          checkAnswer(
            spark.table("t"),
            df.withColumn("p", lit(1)).union(df.withColumn("p", lit(2))))
        }
      }
    }
  }

  test("Refreshes a table in a non-default database - without USE") {
    import org.apache.spark.sql.functions.lit

    withTempDatabase { db =>
      withTempPath { dir =>
        val path = dir.getCanonicalPath

        sql(
          s"""CREATE EXTERNAL TABLE $db.t (id BIGINT)
               |PARTITIONED BY (p INT)
               |STORED AS PARQUET
               |LOCATION '$path'
             """.stripMargin)

        checkAnswer(spark.table(s"$db.t"), spark.emptyDataFrame)

        df.write.parquet(s"$path/p=1")
        sql(s"ALTER TABLE $db.t ADD PARTITION (p=1)")
        sql(s"REFRESH TABLE $db.t")
        checkAnswer(spark.table(s"$db.t"), df.withColumn("p", lit(1)))

        df.write.parquet(s"$path/p=2")
        sql(s"ALTER TABLE $db.t ADD PARTITION (p=2)")
        spark.catalog.refreshTable(s"$db.t")
        checkAnswer(
          spark.table(s"$db.t"),
          df.withColumn("p", lit(1)).union(df.withColumn("p", lit(2))))
      }
    }
  }

  test("invalid database name and table names") {
    {
      val message = intercept[AnalysisException] {
        df.write.format("parquet").saveAsTable("`d:b`.`t:a`")
      }.getMessage
      assert(message.contains("Database 'd:b' not found"))
    }

    {
      val message = intercept[AnalysisException] {
        df.write.format("parquet").saveAsTable("`d:b`.`table`")
      }.getMessage
      assert(message.contains("Database 'd:b' not found"))
    }

    withTempDir { dir =>
      val path = dir.getCanonicalPath

      {
        val message = intercept[AnalysisException] {
          sql(
            s"""
            |CREATE TABLE `d:b`.`t:a` (a int)
            |USING parquet
            |OPTIONS (
            |  path '$path'
            |)
            """.stripMargin)
        }.getMessage
        assert(message.contains("`t:a` is not a valid name for tables/databases. " +
          "Valid names only contain alphabet characters, numbers and _."))
      }

      {
        val message = intercept[AnalysisException] {
          sql(
            s"""
              |CREATE TABLE `d:b`.`table` (a int)
              |USING parquet
              |OPTIONS (
              |  path '$path'
              |)
              """.stripMargin)
        }.getMessage
        assert(message.contains("Database 'd:b' not found"))
      }
    }
  }
}
