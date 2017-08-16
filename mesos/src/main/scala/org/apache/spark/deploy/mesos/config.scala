/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy.mesos

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config.ConfigBuilder

package object config {

  /* Common app configuration. */

  private[spark] val SHUFFLE_CLEANER_INTERVAL_S =
    ConfigBuilder("spark.shuffle.cleaner.interval")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("30s")

  private[spark] val RECOVERY_MODE =
    ConfigBuilder("spark.deploy.recoveryMode")
      .stringConf
      .createWithDefault("NONE")

  private[spark] val DISPATCHER_WEBUI_URL =
    ConfigBuilder("spark.mesos.dispatcher.webui.url")
      .doc("Set the Spark Mesos dispatcher webui_url for interacting with the " +
        "framework. If unset it will point to Spark's internal web UI.")
      .stringConf
      .createOptional

  private[spark] val ZOOKEEPER_URL =
    ConfigBuilder("spark.deploy.zookeeper.url")
      .doc("When `spark.deploy.recoveryMode` is set to ZOOKEEPER, this " +
        "configuration is used to set the zookeeper URL to connect to.")
      .stringConf
      .createOptional

  private[spark] val HISTORY_SERVER_URL =
    ConfigBuilder("spark.mesos.dispatcher.historyServer.url")
      .doc("Set the URL of the history server. The dispatcher will then " +
        "link each driver to its entry in the history server.")
      .stringConf
      .createOptional

}
