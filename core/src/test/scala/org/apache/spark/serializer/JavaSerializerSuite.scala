/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.serializer

import org.apache.spark.{SparkConf, SparkFunSuite}

class JavaSerializerSuite extends SparkFunSuite {
  test("JavaSerializer instances are serializable") {
    val serializer = new JavaSerializer(new SparkConf())
    val instance = serializer.newInstance()
    val obj = instance.deserialize[JavaSerializer](instance.serialize(serializer))
    // enforce class cast
    obj.getClass
  }

  test("Deserialize object containing a primitive Class as attribute") {
    val serializer = new JavaSerializer(new SparkConf())
    val instance = serializer.newInstance()
    val obj = instance.deserialize[ContainsPrimitiveClass](instance.serialize(
      new ContainsPrimitiveClass()))
    // enforce class cast
    obj.getClass
  }
}

private class ContainsPrimitiveClass extends Serializable {
  val intClass = classOf[Int]
  val longClass = classOf[Long]
  val shortClass = classOf[Short]
  val charClass = classOf[Char]
  val doubleClass = classOf[Double]
  val floatClass = classOf[Float]
  val booleanClass = classOf[Boolean]
  val byteClass = classOf[Byte]
  val voidClass = classOf[Void]
}
