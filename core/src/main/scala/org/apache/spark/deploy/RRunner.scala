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

import java.io._
import java.util.concurrent.{Semaphore, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkException, SparkUserAppException}
import org.apache.spark.api.r.{RBackend, RUtils, SparkRDefaults}
import org.apache.spark.util.RedirectThread

/**
 * Main class used to launch SparkR applications using spark-submit. It executes R as a
 * subprocess and then has it connect back to the JVM to access system properties etc.
 */
object RRunner {
  def main(args: Array[String]): Unit = {
    val rFile = PythonRunner.formatPath(args(0))

    val otherArgs = args.slice(1, args.length)

    // Time to wait for SparkR backend to initialize in seconds
    val backendTimeout = sys.env.getOrElse("SPARKR_BACKEND_TIMEOUT", "120").toInt
    val rCommand = {
      // "spark.sparkr.r.command" is deprecated and replaced by "spark.r.command",
      // but kept here for backward compatibility.
      var cmd = sys.props.getOrElse("spark.sparkr.r.command", "Rscript")
      cmd = sys.props.getOrElse("spark.r.command", cmd)
      if (sys.props.getOrElse("spark.submit.deployMode", "client") == "client") {
        cmd = sys.props.getOrElse("spark.r.driver.command", cmd)
      }
      cmd
    }

    //  Connection timeout set by R process on its connection to RBackend in seconds.
    val backendConnectionTimeout = sys.props.getOrElse(
      "spark.r.backendConnectionTimeout", SparkRDefaults.DEFAULT_CONNECTION_TIMEOUT.toString)

    // Check if the file path exists.
    // If not, change directory to current working directory for YARN cluster mode
    val rF = new File(rFile)
    val rFileNormalized = if (!rF.exists()) {
      new Path(rFile).getName
    } else {
      rFile
    }

    // Launch a SparkR backend server for the R process to connect to; this will let it see our
    // Java system properties etc.
    val sparkRBackend = new RBackend()
    @volatile var sparkRBackendPort = 0
    val initialized = new Semaphore(0)
    val sparkRBackendThread = new Thread("SparkR backend") {
      override def run() {
        sparkRBackendPort = sparkRBackend.init()
        initialized.release()
        sparkRBackend.run()
      }
    }

    sparkRBackendThread.start()
    // Wait for RBackend initialization to finish
    if (initialized.tryAcquire(backendTimeout, TimeUnit.SECONDS)) {
      // Launch R
      val returnCode = try {
        val builder = new ProcessBuilder((Seq(rCommand, rFileNormalized) ++ otherArgs).asJava)
        val env = builder.environment()
        env.put("EXISTING_SPARKR_BACKEND_PORT", sparkRBackendPort.toString)
        env.put("SPARKR_BACKEND_CONNECTION_TIMEOUT", backendConnectionTimeout)
        val rPackageDir = RUtils.sparkRPackagePath(isDriver = true)
        // Put the R package directories into an env variable of comma-separated paths
        env.put("SPARKR_PACKAGE_DIR", rPackageDir.mkString(","))
        env.put("R_PROFILE_USER",
          Seq(rPackageDir(0), "SparkR", "profile", "general.R").mkString(File.separator))
        builder.redirectErrorStream(true) // Ugly but needed for stdout and stderr to synchronize
        val process = builder.start()

        new RedirectThread(process.getInputStream, System.out, "redirect R output").start()

        process.waitFor()
      } finally {
        sparkRBackend.close()
      }
      if (returnCode != 0) {
        throw new SparkUserAppException(returnCode)
      }
    } else {
      val errorMessage = s"SparkR backend did not initialize in $backendTimeout seconds"
      // scalastyle:off println
      System.err.println(errorMessage)
      // scalastyle:on println
      throw new SparkException(errorMessage)
    }
  }
}
