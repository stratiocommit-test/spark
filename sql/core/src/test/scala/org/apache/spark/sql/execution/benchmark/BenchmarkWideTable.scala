/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.benchmark

import org.apache.spark.util.Benchmark


/**
 * Benchmark to measure performance for wide table.
 * To run this:
 *  build/sbt "sql/test-only *benchmark.BenchmarkWideTable"
 *
 * Benchmarks in this file are skipped in normal builds.
 */
class BenchmarkWideTable extends BenchmarkBase {

  ignore("project on wide table") {
    val N = 1 << 20
    val df = sparkSession.range(N)
    val columns = (0 until 400).map{ i => s"id as id$i"}
    val benchmark = new Benchmark("projection on wide table", N)
    benchmark.addCase("wide table", numIters = 5) { iter =>
      df.selectExpr(columns : _*).queryExecution.toRdd.count()
    }
    benchmark.run()

    /**
     * Here are some numbers with different split threshold:
     *
     *  Split threshold      methods       Rate(M/s)   Per Row(ns)
     *  10                   400           0.4         2279
     *  100                  200           0.6         1554
     *  1k                   37            0.9         1116
     *  8k                   5             0.5         2025
     *  64k                  1             0.0        21649
     */
  }
}
