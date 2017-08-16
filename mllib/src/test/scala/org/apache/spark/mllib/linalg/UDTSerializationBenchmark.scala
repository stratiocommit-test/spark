/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.linalg

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.util.Benchmark

/**
 * Serialization benchmark for VectorUDT.
 */
object UDTSerializationBenchmark {

  def main(args: Array[String]): Unit = {
    val iters = 1e2.toInt
    val numRows = 1e3.toInt

    val encoder = ExpressionEncoder[Vector].resolveAndBind()

    val vectors = (1 to numRows).map { i =>
      Vectors.dense(Array.fill(1e5.toInt)(1.0 * i))
    }.toArray
    val rows = vectors.map(encoder.toRow)

    val benchmark = new Benchmark("VectorUDT de/serialization", numRows, iters)

    benchmark.addCase("serialize") { _ =>
      var sum = 0
      var i = 0
      while (i < numRows) {
        sum += encoder.toRow(vectors(i)).numFields
        i += 1
      }
    }

    benchmark.addCase("deserialize") { _ =>
      var sum = 0
      var i = 0
      while (i < numRows) {
        sum += encoder.fromRow(rows(i)).numActives
        i += 1
      }
    }

    /*
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.4.11-200.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    VectorUDT de/serialization:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    serialize                                      265 /  318          0.0      265138.5       1.0X
    deserialize                                    155 /  197          0.0      154611.4       1.7X
    */
    benchmark.run()
  }
}
