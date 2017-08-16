/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.internal.config

import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite

class ConfigReaderSuite extends SparkFunSuite {

  test("variable expansion") {
    val env = Map("ENV1" -> "env1")
    val conf = Map("key1" -> "value1", "key2" -> "value2")

    val reader = new ConfigReader(conf.asJava)
    reader.bindEnv(new MapProvider(env.asJava))

    assert(reader.substitute(null) === null)
    assert(reader.substitute("${key1}") === "value1")
    assert(reader.substitute("key1 is: ${key1}") === "key1 is: value1")
    assert(reader.substitute("${key1} ${key2}") === "value1 value2")
    assert(reader.substitute("${key3}") === "${key3}")
    assert(reader.substitute("${env:ENV1}") === "env1")
    assert(reader.substitute("${system:user.name}") === sys.props("user.name"))
    assert(reader.substitute("${key1") === "${key1")

    // Unknown prefixes.
    assert(reader.substitute("${unknown:value}") === "${unknown:value}")
  }

  test("circular references") {
    val conf = Map("key1" -> "${key2}", "key2" -> "${key1}")
    val reader = new ConfigReader(conf.asJava)
    val e = intercept[IllegalArgumentException] {
      reader.substitute("${key1}")
    }
    assert(e.getMessage().contains("Circular"))
  }

  test("spark conf provider filters config keys") {
    val conf = Map("nonspark.key" -> "value", "spark.key" -> "value")
    val reader = new ConfigReader(new SparkConfigProvider(conf.asJava))
    assert(reader.get("nonspark.key") === None)
    assert(reader.get("spark.key") === Some("value"))
  }

}
