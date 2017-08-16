/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.hive.orc

import java.io.File

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.HadoopFsRelationTest
import org.apache.spark.sql.types._

class OrcHadoopFsRelationSuite extends HadoopFsRelationTest {
  import testImplicits._

  override val dataSourceName: String = classOf[OrcFileFormat].getCanonicalName

  // ORC does not play well with NullType and UDT.
  override protected def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: NullType => false
    case _: CalendarIntervalType => false
    case _: UserDefinedType[_] => false
    case _ => true
  }

  test("save()/load() - partitioned table - simple queries - partition columns in data") {
    withTempDir { file =>
      val basePath = new Path(file.getCanonicalPath)
      val fs = basePath.getFileSystem(SparkHadoopUtil.get.conf)
      val qualifiedBasePath = fs.makeQualified(basePath)

      for (p1 <- 1 to 2; p2 <- Seq("foo", "bar")) {
        val partitionDir = new Path(qualifiedBasePath, s"p1=$p1/p2=$p2")
        sparkContext
          .parallelize(for (i <- 1 to 3) yield (i, s"val_$i", p1))
          .toDF("a", "b", "p1")
          .write
          .orc(partitionDir.toString)
      }

      val dataSchemaWithPartition =
        StructType(dataSchema.fields :+ StructField("p1", IntegerType, nullable = true))

      checkQueries(
        spark.read.options(Map(
          "path" -> file.getCanonicalPath,
          "dataSchema" -> dataSchemaWithPartition.json)).format(dataSourceName).load())
    }
  }

  test("SPARK-12218: 'Not' is included in ORC filter pushdown") {
    import testImplicits._

    withSQLConf(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = s"${dir.getCanonicalPath}/table1"
        (1 to 5).map(i => (i, (i % 2).toString)).toDF("a", "b").write.orc(path)

        checkAnswer(
          spark.read.orc(path).where("not (a = 2) or not(b in ('1'))"),
          (1 to 5).map(i => Row(i, (i % 2).toString)))

        checkAnswer(
          spark.read.orc(path).where("not (a = 2 and b in ('1'))"),
          (1 to 5).map(i => Row(i, (i % 2).toString)))
      }
    }
  }

  test("SPARK-13543: Support for specifying compression codec for ORC via option()") {
    withTempPath { dir =>
      val path = s"${dir.getCanonicalPath}/table1"
      val df = (1 to 5).map(i => (i, (i % 2).toString)).toDF("a", "b")
      df.write
        .option("compression", "ZlIb")
        .orc(path)

      // Check if this is compressed as ZLIB.
      val conf = spark.sessionState.newHadoopConf()
      val fs = FileSystem.getLocal(conf)
      val maybeOrcFile = new File(path).listFiles().find(_.getName.endsWith(".zlib.orc"))
      assert(maybeOrcFile.isDefined)
      val orcFilePath = maybeOrcFile.get.toPath.toString
      val expectedCompressionKind =
        OrcFileOperator.getFileReader(orcFilePath).get.getCompression
      assert("ZLIB" === expectedCompressionKind.name())

      val copyDf = spark
        .read
        .orc(path)
      checkAnswer(df, copyDf)
    }
  }

  test("Default compression codec is snappy for ORC compression") {
    withTempPath { file =>
      spark.range(0, 10).write
        .orc(file.getCanonicalPath)
      val expectedCompressionKind =
        OrcFileOperator.getFileReader(file.getCanonicalPath).get.getCompression
      assert("SNAPPY" === expectedCompressionKind.name())
    }
  }
}
