/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.scheduler

/**
 * A simple listener for application events.
 *
 * This listener expects to hear events from a single application only. If events
 * from multiple applications are seen, the behavior is unspecified.
 */
private[spark] class ApplicationEventListener extends SparkListener {
  var appName: Option[String] = None
  var appId: Option[String] = None
  var appAttemptId: Option[String] = None
  var sparkUser: Option[String] = None
  var startTime: Option[Long] = None
  var endTime: Option[Long] = None
  var viewAcls: Option[String] = None
  var adminAcls: Option[String] = None
  var viewAclsGroups: Option[String] = None
  var adminAclsGroups: Option[String] = None

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
    appName = Some(applicationStart.appName)
    appId = applicationStart.appId
    appAttemptId = applicationStart.appAttemptId
    startTime = Some(applicationStart.time)
    sparkUser = Some(applicationStart.sparkUser)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    endTime = Some(applicationEnd.time)
  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate) {
    synchronized {
      val environmentDetails = environmentUpdate.environmentDetails
      val allProperties = environmentDetails("Spark Properties").toMap
      viewAcls = allProperties.get("spark.ui.view.acls")
      adminAcls = allProperties.get("spark.admin.acls")
      viewAclsGroups = allProperties.get("spark.ui.view.acls.groups")
      adminAclsGroups = allProperties.get("spark.admin.acls.groups")
    }
  }
}
