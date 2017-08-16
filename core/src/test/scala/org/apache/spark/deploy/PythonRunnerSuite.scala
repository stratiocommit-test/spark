/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

class PythonRunnerSuite extends SparkFunSuite {

  // Test formatting a single path to be added to the PYTHONPATH
  test("format path") {
    assert(PythonRunner.formatPath("spark.py") === "spark.py")
    assert(PythonRunner.formatPath("file:/spark.py") === "/spark.py")
    assert(PythonRunner.formatPath("file:///spark.py") === "/spark.py")
    assert(PythonRunner.formatPath("local:/spark.py") === "/spark.py")
    assert(PythonRunner.formatPath("local:///spark.py") === "/spark.py")
    if (Utils.isWindows) {
      assert(PythonRunner.formatPath("file:/C:/a/b/spark.py", testWindows = true) ===
        "C:/a/b/spark.py")
      assert(PythonRunner.formatPath("C:\\a\\b\\spark.py", testWindows = true) ===
        "C:/a/b/spark.py")
      assert(PythonRunner.formatPath("C:\\a b\\spark.py", testWindows = true) ===
        "C:/a b/spark.py")
    }
    intercept[IllegalArgumentException] { PythonRunner.formatPath("one:two") }
    intercept[IllegalArgumentException] { PythonRunner.formatPath("hdfs:s3:xtremeFS") }
    intercept[IllegalArgumentException] { PythonRunner.formatPath("hdfs:/path/to/some.py") }
  }

  // Test formatting multiple comma-separated paths to be added to the PYTHONPATH
  test("format paths") {
    assert(PythonRunner.formatPaths("spark.py") === Array("spark.py"))
    assert(PythonRunner.formatPaths("file:/spark.py") === Array("/spark.py"))
    assert(PythonRunner.formatPaths("file:/app.py,local:/spark.py") ===
      Array("/app.py", "/spark.py"))
    assert(PythonRunner.formatPaths("me.py,file:/you.py,local:/we.py") ===
      Array("me.py", "/you.py", "/we.py"))
    if (Utils.isWindows) {
      assert(PythonRunner.formatPaths("C:\\a\\b\\spark.py", testWindows = true) ===
        Array("C:/a/b/spark.py"))
      assert(PythonRunner.formatPaths("C:\\free.py,pie.py", testWindows = true) ===
        Array("C:/free.py", "pie.py"))
      assert(PythonRunner.formatPaths("lovely.py,C:\\free.py,file:/d:/fry.py",
        testWindows = true) ===
        Array("lovely.py", "C:/free.py", "d:/fry.py"))
    }
    intercept[IllegalArgumentException] { PythonRunner.formatPaths("one:two,three") }
    intercept[IllegalArgumentException] { PythonRunner.formatPaths("two,three,four:five:six") }
    intercept[IllegalArgumentException] { PythonRunner.formatPaths("hdfs:/some.py,foo.py") }
    intercept[IllegalArgumentException] { PythonRunner.formatPaths("foo.py,hdfs:/some.py") }
  }
}
