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

import java.net.URLEncoder

import scala.collection.mutable.HashMap
import scala.xml.Node

import org.apache.spark.scheduler.{Schedulable, StageInfo}
import org.apache.spark.ui.UIUtils

/** Table showing list of pools */
private[ui] class PoolTable(pools: Seq[Schedulable], parent: StagesTab) {
  private val listener = parent.progressListener

  def toNodeSeq: Seq[Node] = {
    listener.synchronized {
      poolTable(poolRow, pools)
    }
  }

  private def poolTable(
      makeRow: (Schedulable, HashMap[String, HashMap[Int, StageInfo]]) => Seq[Node],
      rows: Seq[Schedulable]): Seq[Node] = {
    <table class="table table-bordered table-striped table-condensed sortable table-fixed">
      <thead>
        <th>Pool Name</th>
        <th>Minimum Share</th>
        <th>Pool Weight</th>
        <th>Active Stages</th>
        <th>Running Tasks</th>
        <th>SchedulingMode</th>
      </thead>
      <tbody>
        {rows.map(r => makeRow(r, listener.poolToActiveStages))}
      </tbody>
    </table>
  }

  private def poolRow(
      p: Schedulable,
      poolToActiveStages: HashMap[String, HashMap[Int, StageInfo]]): Seq[Node] = {
    val activeStages = poolToActiveStages.get(p.name) match {
      case Some(stages) => stages.size
      case None => 0
    }
    val href = "%s/stages/pool?poolname=%s"
      .format(UIUtils.prependBaseUri(parent.basePath), URLEncoder.encode(p.name, "UTF-8"))
    <tr>
      <td>
        <a href={href}>{p.name}</a>
      </td>
      <td>{p.minShare}</td>
      <td>{p.weight}</td>
      <td>{activeStages}</td>
      <td>{p.runningTasks}</td>
      <td>{p.schedulingMode}</td>
    </tr>
  }
}

