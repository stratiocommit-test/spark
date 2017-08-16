/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.scheduler.cluster

import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.SparkContext
import org.apache.spark.deploy.yarn.{ApplicationMaster, YarnSparkHadoopUtil}
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.util.Utils

private[spark] class YarnClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext)
  extends YarnSchedulerBackend(scheduler, sc) {

  override def start() {
    val attemptId = ApplicationMaster.getAttemptId
    bindToYarn(attemptId.getApplicationId(), Some(attemptId))
    super.start()
    totalExpectedExecutors = YarnSparkHadoopUtil.getInitialTargetExecutorNumber(sc.conf)
  }

  override def getDriverLogUrls: Option[Map[String, String]] = {
    var driverLogs: Option[Map[String, String]] = None
    try {
      val yarnConf = new YarnConfiguration(sc.hadoopConfiguration)
      val containerId = YarnSparkHadoopUtil.get.getContainerId

      val httpAddress = System.getenv(Environment.NM_HOST.name()) +
        ":" + System.getenv(Environment.NM_HTTP_PORT.name())
      // lookup appropriate http scheme for container log urls
      val yarnHttpPolicy = yarnConf.get(
        YarnConfiguration.YARN_HTTP_POLICY_KEY,
        YarnConfiguration.YARN_HTTP_POLICY_DEFAULT
      )
      val user = Utils.getCurrentUserName()
      val httpScheme = if (yarnHttpPolicy == "HTTPS_ONLY") "https://" else "http://"
      val baseUrl = s"$httpScheme$httpAddress/node/containerlogs/$containerId/$user"
      logDebug(s"Base URL for logs: $baseUrl")
      driverLogs = Some(Map(
        "stdout" -> s"$baseUrl/stdout?start=-4096",
        "stderr" -> s"$baseUrl/stderr?start=-4096"))
    } catch {
      case e: Exception =>
        logInfo("Error while building AM log links, so AM" +
          " logs link will not appear in application UI", e)
    }
    driverLogs
  }
}
