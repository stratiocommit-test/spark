/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
import org.apache.spark.sql.SparkSession

/**
 * Entry point in test application for SPARK-8489.
 *
 * This file is not meant to be compiled during tests. It is already included
 * in a pre-built "test.jar" located in the same directory as this file.
 * This is included here for reference only and should NOT be modified without
 * rebuilding the test jar itself.
 *
 * This is used in org.apache.spark.sql.hive.HiveSparkSubmitSuite.
 */
// TODO: actually rebuild this jar with the new changes.
object Main {
  def main(args: Array[String]) {
    // scalastyle:off println
    println("Running regression test for SPARK-8489.")
    val spark = SparkSession.builder
      .master("local")
      .appName("testing")
      .enableHiveSupport()
      .getOrCreate()
    // This line should not throw scala.reflect.internal.MissingRequirementError.
    // See SPARK-8470 for more detail.
    val df = spark.createDataFrame(Seq(MyCoolClass("1", "2", "3")))
    df.collect()
    println("Regression test for SPARK-8489 success!")
    // scalastyle:on println
    spark.stop()
  }
}

