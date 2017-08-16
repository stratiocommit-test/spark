/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ui.jobs

import javax.servlet.http.HttpServletRequest

import org.apache.spark.scheduler.SchedulingMode
import org.apache.spark.ui.{SparkUI, SparkUITab}

/** Web UI showing progress status of all stages in the given SparkContext. */
private[ui] class StagesTab(parent: SparkUI) extends SparkUITab(parent, "stages") {
  val sc = parent.sc
  val conf = parent.conf
  val killEnabled = parent.killEnabled
  val progressListener = parent.jobProgressListener
  val operationGraphListener = parent.operationGraphListener
  val executorsListener = parent.executorsListener

  attachPage(new AllStagesPage(this))
  attachPage(new StagePage(this))
  attachPage(new PoolPage(this))

  def isFairScheduler: Boolean = progressListener.schedulingMode == Some(SchedulingMode.FAIR)

  def handleKillRequest(request: HttpServletRequest): Unit = {
    if (killEnabled && parent.securityManager.checkModifyPermissions(request.getRemoteUser)) {
      val stageId = Option(request.getParameter("id")).map(_.toInt)
      stageId.foreach { id =>
        if (progressListener.activeStages.contains(id)) {
          sc.foreach(_.cancelStage(id))
          // Do a quick pause here to give Spark time to kill the stage so it shows up as
          // killed after the refresh. Note that this will block the serving thread so the
          // time should be limited in duration.
          Thread.sleep(100)
        }
      }
    }
  }

}
