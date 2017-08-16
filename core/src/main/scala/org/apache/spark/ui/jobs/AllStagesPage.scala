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

import scala.xml.{Node, NodeSeq}

import org.apache.spark.scheduler.Schedulable
import org.apache.spark.ui.{UIUtils, WebUIPage}

/** Page showing list of all ongoing and recently finished stages and pools */
private[ui] class AllStagesPage(parent: StagesTab) extends WebUIPage("") {
  private val sc = parent.sc
  private val listener = parent.progressListener
  private def isFairScheduler = parent.isFairScheduler

  def render(request: HttpServletRequest): Seq[Node] = {
    listener.synchronized {
      val activeStages = listener.activeStages.values.toSeq
      val pendingStages = listener.pendingStages.values.toSeq
      val completedStages = listener.completedStages.reverse.toSeq
      val numCompletedStages = listener.numCompletedStages
      val failedStages = listener.failedStages.reverse.toSeq
      val numFailedStages = listener.numFailedStages
      val subPath = "stages"

      val activeStagesTable =
        new StageTableBase(request, activeStages, "active", "activeStage", parent.basePath, subPath,
          parent.progressListener, parent.isFairScheduler,
          killEnabled = parent.killEnabled, isFailedStage = false)
      val pendingStagesTable =
        new StageTableBase(request, pendingStages, "pending", "pendingStage", parent.basePath,
          subPath, parent.progressListener, parent.isFairScheduler,
          killEnabled = false, isFailedStage = false)
      val completedStagesTable =
        new StageTableBase(request, completedStages, "completed", "completedStage", parent.basePath,
          subPath, parent.progressListener, parent.isFairScheduler,
          killEnabled = false, isFailedStage = false)
      val failedStagesTable =
        new StageTableBase(request, failedStages, "failed", "failedStage", parent.basePath, subPath,
          parent.progressListener, parent.isFairScheduler,
          killEnabled = false, isFailedStage = true)

      // For now, pool information is only accessible in live UIs
      val pools = sc.map(_.getAllPools).getOrElse(Seq.empty[Schedulable])
      val poolTable = new PoolTable(pools, parent)

      val shouldShowActiveStages = activeStages.nonEmpty
      val shouldShowPendingStages = pendingStages.nonEmpty
      val shouldShowCompletedStages = completedStages.nonEmpty
      val shouldShowFailedStages = failedStages.nonEmpty

      val completedStageNumStr = if (numCompletedStages == completedStages.size) {
        s"$numCompletedStages"
      } else {
        s"$numCompletedStages, only showing ${completedStages.size}"
      }

      val summary: NodeSeq =
        <div>
          <ul class="unstyled">
            {
              if (shouldShowActiveStages) {
                <li>
                  <a href="#active"><strong>Active Stages:</strong></a>
                  {activeStages.size}
                </li>
              }
            }
            {
              if (shouldShowPendingStages) {
                <li>
                  <a href="#pending"><strong>Pending Stages:</strong></a>
                  {pendingStages.size}
                </li>
              }
            }
            {
              if (shouldShowCompletedStages) {
                <li id="completed-summary">
                  <a href="#completed"><strong>Completed Stages:</strong></a>
                  {completedStageNumStr}
                </li>
              }
            }
            {
              if (shouldShowFailedStages) {
                <li>
                  <a href="#failed"><strong>Failed Stages:</strong></a>
                  {numFailedStages}
                </li>
              }
            }
          </ul>
        </div>

      var content = summary ++
        {
          if (sc.isDefined && isFairScheduler) {
            <h4>{pools.size} Fair Scheduler Pools</h4> ++ poolTable.toNodeSeq
          } else {
            Seq[Node]()
          }
        }
      if (shouldShowActiveStages) {
        content ++= <h4 id="active">Active Stages ({activeStages.size})</h4> ++
        activeStagesTable.toNodeSeq
      }
      if (shouldShowPendingStages) {
        content ++= <h4 id="pending">Pending Stages ({pendingStages.size})</h4> ++
        pendingStagesTable.toNodeSeq
      }
      if (shouldShowCompletedStages) {
        content ++= <h4 id="completed">Completed Stages ({completedStageNumStr})</h4> ++
        completedStagesTable.toNodeSeq
      }
      if (shouldShowFailedStages) {
        content ++= <h4 id ="failed">Failed Stages ({numFailedStages})</h4> ++
        failedStagesTable.toNodeSeq
      }
      UIUtils.headerSparkPage("Stages for All Jobs", content, parent)
    }
  }
}

