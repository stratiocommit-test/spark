/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy.mesos.ui

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.scheduler.cluster.mesos.MesosClusterScheduler
import org.apache.spark.ui.{SparkUI, WebUI}
import org.apache.spark.ui.JettyUtils._

/**
 * UI that displays driver results from the [[org.apache.spark.deploy.mesos.MesosClusterDispatcher]]
 */
private[spark] class MesosClusterUI(
    securityManager: SecurityManager,
    port: Int,
    val conf: SparkConf,
    dispatcherPublicAddress: String,
    val scheduler: MesosClusterScheduler)
  extends WebUI(securityManager, securityManager.getSSLOptions("mesos"), port, conf) {

  initialize()

  def activeWebUiUrl: String = "http://" + dispatcherPublicAddress + ":" + boundPort

  override def initialize() {
    attachPage(new MesosClusterPage(this))
    attachPage(new DriverPage(this))
    attachHandler(createStaticHandler(MesosClusterUI.STATIC_RESOURCE_DIR, "/static"))
  }
}

private object MesosClusterUI {
  val STATIC_RESOURCE_DIR = SparkUI.STATIC_RESOURCE_DIR
}
