/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ui

import java.util.concurrent.Semaphore

import scala.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler.SchedulingMode

// scalastyle:off
/**
 * Continuously generates jobs that expose various features of the WebUI (internal testing tool).
 *
 * Usage: ./bin/spark-class org.apache.spark.ui.UIWorkloadGenerator [master] [FIFO|FAIR] [#job set (4 jobs per set)]
 */
// scalastyle:on
private[spark] object UIWorkloadGenerator {

  val NUM_PARTITIONS = 100
  val INTER_JOB_WAIT_MS = 5000

  def main(args: Array[String]) {
    if (args.length < 3) {
      // scalastyle:off println
      println(
        "Usage: ./bin/spark-class org.apache.spark.ui.UIWorkloadGenerator " +
          "[master] [FIFO|FAIR] [#job set (4 jobs per set)]")
      // scalastyle:on println
      System.exit(1)
    }

    val conf = new SparkConf().setMaster(args(0)).setAppName("Spark UI tester")

    val schedulingMode = SchedulingMode.withName(args(1))
    if (schedulingMode == SchedulingMode.FAIR) {
      conf.set("spark.scheduler.mode", "FAIR")
    }
    val nJobSet = args(2).toInt
    val sc = new SparkContext(conf)

    def setProperties(s: String): Unit = {
      if (schedulingMode == SchedulingMode.FAIR) {
        sc.setLocalProperty("spark.scheduler.pool", s)
      }
      sc.setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, s)
    }

    val baseData = sc.makeRDD(1 to NUM_PARTITIONS * 10, NUM_PARTITIONS)
    def nextFloat(): Float = new Random().nextFloat()

    val jobs = Seq[(String, () => Long)](
      ("Count", baseData.count),
      ("Cache and Count", baseData.map(x => x).cache().count),
      ("Single Shuffle", baseData.map(x => (x % 10, x)).reduceByKey(_ + _).count),
      ("Entirely failed phase", baseData.map(x => throw new Exception).count),
      ("Partially failed phase", {
        baseData.map{x =>
          val probFailure = (4.0 / NUM_PARTITIONS)
          if (nextFloat() < probFailure) {
            throw new Exception("This is a task failure")
          }
          1
        }.count
      }),
      ("Partially failed phase (longer tasks)", {
        baseData.map{x =>
          val probFailure = (4.0 / NUM_PARTITIONS)
          if (nextFloat() < probFailure) {
            Thread.sleep(100)
            throw new Exception("This is a task failure")
          }
          1
        }.count
      }),
      ("Job with delays", baseData.map(x => Thread.sleep(100)).count)
    )

    val barrier = new Semaphore(-nJobSet * jobs.size + 1)

    (1 to nJobSet).foreach { _ =>
      for ((desc, job) <- jobs) {
        new Thread {
          override def run() {
            // scalastyle:off println
            try {
              setProperties(desc)
              job()
              println("Job finished: " + desc)
            } catch {
              case e: Exception =>
                println("Job Failed: " + desc)
            } finally {
              barrier.release()
            }
            // scalastyle:on println
          }
        }.start
        Thread.sleep(INTER_JOB_WAIT_MS)
      }
    }

    // Waiting for threads.
    barrier.acquire()
    sc.stop()
  }
}
