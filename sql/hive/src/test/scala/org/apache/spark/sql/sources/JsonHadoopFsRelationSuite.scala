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

import java.math.BigDecimal

import org.apache.hadoop.fs.Path

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class JsonHadoopFsRelationSuite extends HadoopFsRelationTest {
  override val dataSourceName: String = "json"

  // JSON does not write data of NullType and does not play well with BinaryType.
  override protected def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: NullType => false
    case _: BinaryType => false
    case _: CalendarIntervalType => false
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
          .parallelize(for (i <- 1 to 3) yield s"""{"a":$i,"b":"val_$i"}""")
          .saveAsTextFile(partitionDir.toString)
      }

      val dataSchemaWithPartition =
        StructType(dataSchema.fields :+ StructField("p1", IntegerType, nullable = true))

      checkQueries(
        spark.read.format(dataSourceName)
          .option("dataSchema", dataSchemaWithPartition.json)
          .load(file.getCanonicalPath))
    }
  }

  test("SPARK-9894: save complex types to JSON") {
    withTempDir { file =>
      file.delete()

      val schema =
        new StructType()
          .add("array", ArrayType(LongType))
          .add("map", MapType(StringType, new StructType().add("innerField", LongType)))

      val data =
        Row(Seq(1L, 2L, 3L), Map("m1" -> Row(4L))) ::
          Row(Seq(5L, 6L, 7L), Map("m2" -> Row(10L))) :: Nil
      val df = spark.createDataFrame(sparkContext.parallelize(data), schema)

      // Write the data out.
      df.write.format(dataSourceName).save(file.getCanonicalPath)

      // Read it back and check the result.
      checkAnswer(
        spark.read.format(dataSourceName).schema(schema).load(file.getCanonicalPath),
        df
      )
    }
  }

  test("SPARK-10196: save decimal type to JSON") {
    withTempDir { file =>
      file.delete()

      val schema =
        new StructType()
          .add("decimal", DecimalType(7, 2))

      val data =
        Row(new BigDecimal("10.02")) ::
          Row(new BigDecimal("20000.99")) ::
          Row(new BigDecimal("10000")) :: Nil
      val df = spark.createDataFrame(sparkContext.parallelize(data), schema)

      // Write the data out.
      df.write.format(dataSourceName).save(file.getCanonicalPath)

      // Read it back and check the result.
      checkAnswer(
        spark.read.format(dataSourceName).schema(schema).load(file.getCanonicalPath),
        df
      )
    }
  }
}
