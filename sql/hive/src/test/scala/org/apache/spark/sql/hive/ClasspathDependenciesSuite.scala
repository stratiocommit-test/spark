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

import java.net.URL

import org.apache.spark.SparkFunSuite

/**
 * Verify that some classes load and that others are not found on the classpath.
 *
 * This is used to detect classpath and shading conflicts.
 */
class ClasspathDependenciesSuite extends SparkFunSuite {
  private val classloader = this.getClass.getClassLoader

  private def assertLoads(classname: String): Unit = {
    val resourceURL: URL = Option(findResource(classname)).getOrElse {
      fail(s"Class $classname not found as ${resourceName(classname)}")
    }

    logInfo(s"Class $classname at $resourceURL")
    classloader.loadClass(classname)
  }

  private def findResource(classname: String): URL = {
    val resource = resourceName(classname)
    classloader.getResource(resource)
  }

  private def resourceName(classname: String): String = {
    classname.replace(".", "/") + ".class"
  }

  private def assertClassNotFound(classname: String): Unit = {
    Option(findResource(classname)).foreach { resourceURL =>
      fail(s"Class $classname found at $resourceURL")
    }

    intercept[ClassNotFoundException] {
      classloader.loadClass(classname)
    }
  }

  test("shaded Protobuf") {
    assertLoads("org.apache.hive.com.google.protobuf.ServiceException")
  }

  test("shaded Kryo") {
    assertLoads("org.apache.hive.com.esotericsoftware.kryo.Kryo")
  }

  test("hive-common") {
    assertLoads("org.apache.hadoop.hive.conf.HiveConf")
  }

  test("hive-exec") {
    assertLoads("org.apache.hadoop.hive.ql.CommandNeedRetryException")
  }

  private val STD_INSTANTIATOR = "org.objenesis.strategy.StdInstantiatorStrategy"

  test("Forbidden Dependencies") {
    assertClassNotFound("com.esotericsoftware.shaded." + STD_INSTANTIATOR)
    assertClassNotFound("org.apache.hive.com.esotericsoftware.shaded." + STD_INSTANTIATOR)
  }

  test("parquet-hadoop-bundle") {
    assertLoads("parquet.hadoop.ParquetOutputFormat")
    assertLoads("parquet.hadoop.ParquetInputFormat")
  }
}
