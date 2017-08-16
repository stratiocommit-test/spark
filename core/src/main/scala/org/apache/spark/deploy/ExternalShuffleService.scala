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

import java.util.concurrent.CountDownLatch

import scala.collection.JavaConverters._

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.TransportContext
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.sasl.SaslServerBootstrap
import org.apache.spark.network.server.{TransportServer, TransportServerBootstrap}
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler
import org.apache.spark.network.util.TransportConf
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * Provides a server from which Executors can read shuffle files (rather than reading directly from
 * each other), to provide uninterrupted access to the files in the face of executors being turned
 * off or killed.
 *
 * Optionally requires SASL authentication in order to read. See [[SecurityManager]].
 */
private[deploy]
class ExternalShuffleService(sparkConf: SparkConf, securityManager: SecurityManager)
  extends Logging {
  protected val masterMetricsSystem =
    MetricsSystem.createMetricsSystem("shuffleService", sparkConf, securityManager)

  private val enabled = sparkConf.getBoolean("spark.shuffle.service.enabled", false)
  private val port = sparkConf.getInt("spark.shuffle.service.port", 7337)
  private val useSasl: Boolean = securityManager.isAuthenticationEnabled()

  private val transportConf =
    SparkTransportConf.fromSparkConf(sparkConf, "shuffle", numUsableCores = 0)
  private val blockHandler = newShuffleBlockHandler(transportConf)
  private val transportContext: TransportContext =
    new TransportContext(transportConf, blockHandler, true)

  private var server: TransportServer = _

  private val shuffleServiceSource = new ExternalShuffleServiceSource(blockHandler)

  /** Create a new shuffle block handler. Factored out for subclasses to override. */
  protected def newShuffleBlockHandler(conf: TransportConf): ExternalShuffleBlockHandler = {
    new ExternalShuffleBlockHandler(conf, null)
  }

  /** Starts the external shuffle service if the user has configured us to. */
  def startIfEnabled() {
    if (enabled) {
      start()
    }
  }

  /** Start the external shuffle service */
  def start() {
    require(server == null, "Shuffle server already started")
    logInfo(s"Starting shuffle service on port $port with useSasl = $useSasl")
    val bootstraps: Seq[TransportServerBootstrap] =
      if (useSasl) {
        Seq(new SaslServerBootstrap(transportConf, securityManager))
      } else {
        Nil
      }
    server = transportContext.createServer(port, bootstraps.asJava)

    masterMetricsSystem.registerSource(shuffleServiceSource)
    masterMetricsSystem.start()
  }

  /** Clean up all shuffle files associated with an application that has exited. */
  def applicationRemoved(appId: String): Unit = {
    blockHandler.applicationRemoved(appId, true /* cleanupLocalDirs */)
  }

  def stop() {
    if (server != null) {
      server.close()
      server = null
    }
  }
}

/**
 * A main class for running the external shuffle service.
 */
object ExternalShuffleService extends Logging {
  @volatile
  private var server: ExternalShuffleService = _

  private val barrier = new CountDownLatch(1)

  def main(args: Array[String]): Unit = {
    main(args, (conf: SparkConf, sm: SecurityManager) => new ExternalShuffleService(conf, sm))
  }

  /** A helper main method that allows the caller to call this with a custom shuffle service. */
  private[spark] def main(
      args: Array[String],
      newShuffleService: (SparkConf, SecurityManager) => ExternalShuffleService): Unit = {
    Utils.initDaemon(log)
    val sparkConf = new SparkConf
    Utils.loadDefaultSparkProperties(sparkConf)
    val securityManager = new SecurityManager(sparkConf)

    // we override this value since this service is started from the command line
    // and we assume the user really wants it to be running
    sparkConf.set("spark.shuffle.service.enabled", "true")
    server = newShuffleService(sparkConf, securityManager)
    server.start()

    logDebug("Adding shutdown hook") // force eager creation of logger
    ShutdownHookManager.addShutdownHook { () =>
      logInfo("Shutting down shuffle service.")
      server.stop()
      barrier.countDown()
    }

    // keep running until the process is terminated
    barrier.await()
  }
}
