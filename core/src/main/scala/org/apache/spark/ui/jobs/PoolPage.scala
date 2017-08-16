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

import scala.xml.Node

import org.apache.spark.scheduler.StageInfo
import org.apache.spark.ui.{UIUtils, WebUIPage}

/** Page showing specific pool details */
private[ui] class PoolPage(parent: StagesTab) extends WebUIPage("pool") {
  private val sc = parent.sc
  private val listener = parent.progressListener

  def render(request: HttpServletRequest): Seq[Node] = {
    listener.synchronized {
      val poolName = Option(request.getParameter("poolname")).map { poolname =>
        UIUtils.decodeURLParameter(poolname)
      }.getOrElse {
        throw new IllegalArgumentException(s"Missing poolname parameter")
      }

      val poolToActiveStages = listener.poolToActiveStages
      val activeStages = poolToActiveStages.get(poolName) match {
        case Some(s) => s.values.toSeq
        case None => Seq[StageInfo]()
      }
      val shouldShowActiveStages = activeStages.nonEmpty
      val activeStagesTable =
        new StageTableBase(request, activeStages, "", "activeStage", parent.basePath, "stages/pool",
          parent.progressListener, parent.isFairScheduler, parent.killEnabled,
          isFailedStage = false)

      // For now, pool information is only accessible in live UIs
      val pools = sc.map(_.getPoolForName(poolName).getOrElse {
        throw new IllegalArgumentException(s"Unknown poolname: $poolName")
      }).toSeq
      val poolTable = new PoolTable(pools, parent)

      var content = <h4>Summary </h4> ++ poolTable.toNodeSeq
      if (shouldShowActiveStages) {
        content ++= <h4>{activeStages.size} Active Stages</h4> ++ activeStagesTable.toNodeSeq
      }

      UIUtils.headerSparkPage("Fair Scheduler Pool: " + poolName, content, parent)
    }
  }
}
