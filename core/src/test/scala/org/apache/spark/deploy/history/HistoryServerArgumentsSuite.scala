/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy.history

import java.io.File
import java.nio.charset.StandardCharsets._

import com.google.common.io.Files

import org.apache.spark._
import org.apache.spark.util.Utils

class HistoryServerArgumentsSuite extends SparkFunSuite {

  private val logDir = new File("src/test/resources/spark-events")
  private val conf = new SparkConf()
    .set("spark.history.fs.logDirectory", logDir.getAbsolutePath)
    .set("spark.history.fs.updateInterval", "1")
    .set("spark.testing", "true")

  test("No Arguments Parsing") {
    val argStrings = Array.empty[String]
    val hsa = new HistoryServerArguments(conf, argStrings)
    assert(conf.get("spark.history.fs.logDirectory") === logDir.getAbsolutePath)
    assert(conf.get("spark.history.fs.updateInterval") === "1")
    assert(conf.get("spark.testing") === "true")
  }

  test("Directory Arguments Parsing --dir or -d") {
    val argStrings = Array("--dir", "src/test/resources/spark-events1")
    val hsa = new HistoryServerArguments(conf, argStrings)
    assert(conf.get("spark.history.fs.logDirectory") === "src/test/resources/spark-events1")
  }

  test("Directory Param can also be set directly") {
    val argStrings = Array("src/test/resources/spark-events2")
    val hsa = new HistoryServerArguments(conf, argStrings)
    assert(conf.get("spark.history.fs.logDirectory") === "src/test/resources/spark-events2")
  }

  test("Properties File Arguments Parsing --properties-file") {
    val tmpDir = Utils.createTempDir()
    val outFile = File.createTempFile("test-load-spark-properties", "test", tmpDir)
    try {
      Files.write("spark.test.CustomPropertyA blah\n" +
        "spark.test.CustomPropertyB notblah\n", outFile, UTF_8)
      val argStrings = Array("--properties-file", outFile.getAbsolutePath)
      val hsa = new HistoryServerArguments(conf, argStrings)
      assert(conf.get("spark.test.CustomPropertyA") === "blah")
      assert(conf.get("spark.test.CustomPropertyB") === "notblah")
    } finally {
      Utils.deleteRecursively(tmpDir)
    }
  }

}
