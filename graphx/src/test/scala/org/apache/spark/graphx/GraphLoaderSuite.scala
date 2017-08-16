/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.graphx

import java.io.File
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

class GraphLoaderSuite extends SparkFunSuite with LocalSparkContext {

  test("GraphLoader.edgeListFile") {
    withSpark { sc =>
      val tmpDir = Utils.createTempDir()
      val graphFile = new File(tmpDir.getAbsolutePath, "graph.txt")
      val writer = new OutputStreamWriter(new FileOutputStream(graphFile), StandardCharsets.UTF_8)
      for (i <- (1 until 101)) writer.write(s"$i 0\n")
      writer.close()
      try {
        val graph = GraphLoader.edgeListFile(sc, tmpDir.getAbsolutePath)
        val neighborAttrSums = graph.aggregateMessages[Int](
          ctx => ctx.sendToDst(ctx.srcAttr),
          _ + _)
        assert(neighborAttrSums.collect.toSet === Set((0: VertexId, 100)))
      } finally {
        Utils.deleteRecursively(tmpDir)
      }
    }
  }
}
