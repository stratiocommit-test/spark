/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.datasources.text

import java.io.File

import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.GzipCodec

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row, SaveMode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.Utils

class TextSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("reading text file") {
    verifyFrame(spark.read.format("text").load(testFile))
  }

  test("SQLContext.read.text() API") {
    verifyFrame(spark.read.text(testFile))
  }

  test("SPARK-12562 verify write.text() can handle column name beyond `value`") {
    val df = spark.read.text(testFile).withColumnRenamed("value", "adwrasdf")

    val tempFile = Utils.createTempDir()
    tempFile.delete()
    df.write.text(tempFile.getCanonicalPath)
    verifyFrame(spark.read.text(tempFile.getCanonicalPath))

    Utils.deleteRecursively(tempFile)
  }

  test("error handling for invalid schema") {
    val tempFile = Utils.createTempDir()
    tempFile.delete()

    val df = spark.range(2)
    intercept[AnalysisException] {
      df.write.text(tempFile.getCanonicalPath)
    }

    intercept[AnalysisException] {
      spark.range(2).select(df("id"), df("id") + 1).write.text(tempFile.getCanonicalPath)
    }
  }

  test("reading partitioned data using read.textFile()") {
    val partitionedData = Thread.currentThread().getContextClassLoader
      .getResource("test-data/text-partitioned").toString
    val ds = spark.read.textFile(partitionedData)
    val data = ds.collect()

    assert(ds.schema == new StructType().add("value", StringType))
    assert(data.length == 2)
  }

  test("support for partitioned reading using read.text()") {
    val partitionedData = Thread.currentThread().getContextClassLoader
      .getResource("test-data/text-partitioned").toString
    val df = spark.read.text(partitionedData)
    val data = df.filter("year = '2015'").select("value").collect()

    assert(data(0) == Row("2015-test"))
    assert(data.length == 1)
  }

  test("SPARK-13503 Support to specify the option for compression codec for TEXT") {
    val testDf = spark.read.text(testFile)
    val extensionNameMap = Map("bzip2" -> ".bz2", "deflate" -> ".deflate", "gzip" -> ".gz")
    extensionNameMap.foreach {
      case (codecName, extension) =>
        val tempDir = Utils.createTempDir()
        val tempDirPath = tempDir.getAbsolutePath
        testDf.write.option("compression", codecName).mode(SaveMode.Overwrite).text(tempDirPath)
        val compressedFiles = new File(tempDirPath).listFiles()
        assert(compressedFiles.exists(_.getName.endsWith(s".txt$extension")))
        verifyFrame(spark.read.text(tempDirPath))
    }

    val errMsg = intercept[IllegalArgumentException] {
      val tempDirPath = Utils.createTempDir().getAbsolutePath
      testDf.write.option("compression", "illegal").mode(SaveMode.Overwrite).text(tempDirPath)
    }
    assert(errMsg.getMessage.contains("Codec [illegal] is not available. " +
      "Known codecs are"))
  }

  test("SPARK-13543 Write the output as uncompressed via option()") {
    val extraOptions = Map[String, String](
      "mapreduce.output.fileoutputformat.compress" -> "true",
      "mapreduce.output.fileoutputformat.compress.type" -> CompressionType.BLOCK.toString,
      "mapreduce.map.output.compress" -> "true",
      "mapreduce.output.fileoutputformat.compress.codec" -> classOf[GzipCodec].getName,
      "mapreduce.map.output.compress.codec" -> classOf[GzipCodec].getName
    )
    withTempDir { dir =>
      val testDf = spark.read.text(testFile)
      val tempDir = Utils.createTempDir()
      val tempDirPath = tempDir.getAbsolutePath
      testDf.write.option("compression", "none")
        .options(extraOptions).mode(SaveMode.Overwrite).text(tempDirPath)
      val compressedFiles = new File(tempDirPath).listFiles()
      assert(compressedFiles.exists(!_.getName.endsWith(".txt.gz")))
      verifyFrame(spark.read.options(extraOptions).text(tempDirPath))
    }
  }

  test("SPARK-14343: select partitioning column") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val ds1 = spark.range(1).selectExpr("CONCAT('val_', id)")
      ds1.write.text(s"$path/part=a")
      ds1.write.text(s"$path/part=b")

      checkAnswer(
        spark.read.format("text").load(path).select($"part"),
        Row("a") :: Row("b") :: Nil)
    }
  }

  test("SPARK-15654: should not split gz files") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val df1 = spark.range(0, 1000).selectExpr("CAST(id AS STRING) AS s")
      df1.write.option("compression", "gzip").mode("overwrite").text(path)

      val expected = df1.collect()
      Seq(10, 100, 1000).foreach { bytes =>
        withSQLConf(SQLConf.FILES_MAX_PARTITION_BYTES.key -> bytes.toString) {
          val df2 = spark.read.format("text").load(path)
          checkAnswer(df2, expected)
        }
      }
    }
  }

  private def testFile: String = {
    Thread.currentThread().getContextClassLoader.getResource("test-data/text-suite.txt").toString
  }

  /** Verifies data and schema. */
  private def verifyFrame(df: DataFrame): Unit = {
    // schema
    assert(df.schema == new StructType().add("value", StringType))

    // verify content
    val data = df.collect()
    assert(data(0) == Row("This is a test file for the text data source"))
    assert(data(1) == Row("1+1"))
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    // scalastyle:off
    assert(data(2) == Row("数据砖头"))
    // scalastyle:on
    assert(data(3) == Row("\"doh\""))
    assert(data.length == 4)
  }
}
