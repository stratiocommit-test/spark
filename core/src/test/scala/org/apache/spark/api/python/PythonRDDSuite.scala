/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.api.python

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkFunSuite

class PythonRDDSuite extends SparkFunSuite {

  test("Writing large strings to the worker") {
    val input: List[String] = List("a"*100000)
    val buffer = new DataOutputStream(new ByteArrayOutputStream)
    PythonRDD.writeIteratorToStream(input.iterator, buffer)
  }

  test("Handle nulls gracefully") {
    val buffer = new DataOutputStream(new ByteArrayOutputStream)
    // Should not have NPE when write an Iterator with null in it
    // The correctness will be tested in Python
    PythonRDD.writeIteratorToStream(Iterator("a", null), buffer)
    PythonRDD.writeIteratorToStream(Iterator(null, "a"), buffer)
    PythonRDD.writeIteratorToStream(Iterator("a".getBytes(StandardCharsets.UTF_8), null), buffer)
    PythonRDD.writeIteratorToStream(Iterator(null, "a".getBytes(StandardCharsets.UTF_8)), buffer)
    PythonRDD.writeIteratorToStream(Iterator((null, null), ("a", null), (null, "b")), buffer)
    PythonRDD.writeIteratorToStream(Iterator(
      (null, null),
      ("a".getBytes(StandardCharsets.UTF_8), null),
      (null, "b".getBytes(StandardCharsets.UTF_8))), buffer)
  }
}
