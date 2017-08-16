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

import java.io.{File, PrintWriter}

import scala.io.Source

import org.scalatest.Matchers

import org.apache.spark.{SharedSparkContext, SparkConf, SparkFunSuite}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.Utils

// This test suite uses SharedSparkContext because we need a SparkEnv in order to deserialize
// a PythonBroadcast:
class PythonBroadcastSuite extends SparkFunSuite with Matchers with SharedSparkContext {
  test("PythonBroadcast can be serialized with Kryo (SPARK-4882)") {
    val tempDir = Utils.createTempDir()
    val broadcastedString = "Hello, world!"
    def assertBroadcastIsValid(broadcast: PythonBroadcast): Unit = {
      val source = Source.fromFile(broadcast.path)
      val contents = source.mkString
      source.close()
      contents should be (broadcastedString)
    }
    try {
      val broadcastDataFile: File = {
        val file = new File(tempDir, "broadcastData")
        val printWriter = new PrintWriter(file)
        printWriter.write(broadcastedString)
        printWriter.close()
        file
      }
      val broadcast = new PythonBroadcast(broadcastDataFile.getAbsolutePath)
      assertBroadcastIsValid(broadcast)
      val conf = new SparkConf().set("spark.kryo.registrationRequired", "true")
      val deserializedBroadcast =
        Utils.clone[PythonBroadcast](broadcast, new KryoSerializer(conf).newInstance())
      assertBroadcastIsValid(deserializedBroadcast)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}
