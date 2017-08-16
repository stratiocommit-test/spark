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

import scala.collection.mutable
import scala.xml.{Node, Unparsed}

import org.apache.spark.ui.{ToolTips, UIUtils}
import org.apache.spark.ui.jobs.UIData.StageUIData
import org.apache.spark.util.Utils

/** Stage summary grouped by executors. */
private[ui] class ExecutorTable(stageId: Int, stageAttemptId: Int, parent: StagesTab) {
  private val listener = parent.progressListener

  def toNodeSeq: Seq[Node] = {
    listener.synchronized {
      executorTable()
    }
  }

  /** Special table which merges two header cells. */
  private def executorTable[T](): Seq[Node] = {
    val stageData = listener.stageIdToData.get((stageId, stageAttemptId))
    var hasInput = false
    var hasOutput = false
    var hasShuffleWrite = false
    var hasShuffleRead = false
    var hasBytesSpilled = false
    stageData.foreach { data =>
        hasInput = data.hasInput
        hasOutput = data.hasOutput
        hasShuffleRead = data.hasShuffleRead
        hasShuffleWrite = data.hasShuffleWrite
        hasBytesSpilled = data.hasBytesSpilled
    }

    <table class={UIUtils.TABLE_CLASS_STRIPED_SORTABLE}>
      <thead>
        <th id="executorid">Executor ID</th>
        <th>Address</th>
        <th>Task Time</th>
        <th>Total Tasks</th>
        <th>Failed Tasks</th>
        <th>Killed Tasks</th>
        <th>Succeeded Tasks</th>
        {if (hasInput) {
          <th>
            <span data-toggle="tooltip" title={ToolTips.INPUT}>Input Size / Records</span>
          </th>
        }}
        {if (hasOutput) {
          <th>
            <span data-toggle="tooltip" title={ToolTips.OUTPUT}>Output Size / Records</span>
          </th>
        }}
        {if (hasShuffleRead) {
          <th>
            <span data-toggle="tooltip" title={ToolTips.SHUFFLE_READ}>
            Shuffle Read Size / Records</span>
          </th>
        }}
        {if (hasShuffleWrite) {
          <th>
            <span data-toggle="tooltip" title={ToolTips.SHUFFLE_WRITE}>
            Shuffle Write Size / Records</span>
          </th>
        }}
        {if (hasBytesSpilled) {
          <th>Shuffle Spill (Memory)</th>
          <th>Shuffle Spill (Disk)</th>
        }}
      </thead>
      <tbody>
        {createExecutorTable()}
      </tbody>
    </table>
    <script>
      {Unparsed {
        """
          |      window.onload = function() {
          |        sorttable.innerSortFunction.apply(document.getElementById('executorid'), [])
          |      };
        """.stripMargin
      }}
    </script>
  }

  private def createExecutorTable() : Seq[Node] = {
    // Make an executor-id -> address map
    val executorIdToAddress = mutable.HashMap[String, String]()
    listener.blockManagerIds.foreach { blockManagerId =>
      val address = blockManagerId.hostPort
      val executorId = blockManagerId.executorId
      executorIdToAddress.put(executorId, address)
    }

    listener.stageIdToData.get((stageId, stageAttemptId)) match {
      case Some(stageData: StageUIData) =>
        stageData.executorSummary.toSeq.sortBy(_._1).map { case (k, v) =>
          <tr>
            <td>
              <div style="float: left">{k}</div>
              <div style="float: right">
              {
                val logs = parent.executorsListener.executorToTaskSummary.get(k)
                  .map(_.executorLogs).getOrElse(Map.empty)
                logs.map {
                  case (logName, logUrl) => <div><a href={logUrl}>{logName}</a></div>
                }
              }
              </div>
            </td>
            <td>{executorIdToAddress.getOrElse(k, "CANNOT FIND ADDRESS")}</td>
            <td sorttable_customkey={v.taskTime.toString}>{UIUtils.formatDuration(v.taskTime)}</td>
            <td>{v.failedTasks + v.succeededTasks + v.killedTasks}</td>
            <td>{v.failedTasks}</td>
            <td>{v.killedTasks}</td>
            <td>{v.succeededTasks}</td>
            {if (stageData.hasInput) {
              <td sorttable_customkey={v.inputBytes.toString}>
                {s"${Utils.bytesToString(v.inputBytes)} / ${v.inputRecords}"}
              </td>
            }}
            {if (stageData.hasOutput) {
              <td sorttable_customkey={v.outputBytes.toString}>
                {s"${Utils.bytesToString(v.outputBytes)} / ${v.outputRecords}"}
              </td>
            }}
            {if (stageData.hasShuffleRead) {
              <td sorttable_customkey={v.shuffleRead.toString}>
                {s"${Utils.bytesToString(v.shuffleRead)} / ${v.shuffleReadRecords}"}
              </td>
            }}
            {if (stageData.hasShuffleWrite) {
              <td sorttable_customkey={v.shuffleWrite.toString}>
                {s"${Utils.bytesToString(v.shuffleWrite)} / ${v.shuffleWriteRecords}"}
              </td>
            }}
            {if (stageData.hasBytesSpilled) {
              <td sorttable_customkey={v.memoryBytesSpilled.toString}>
                {Utils.bytesToString(v.memoryBytesSpilled)}
              </td>
              <td sorttable_customkey={v.diskBytesSpilled.toString}>
                {Utils.bytesToString(v.diskBytesSpilled)}
              </td>
            }}
          </tr>
        }
      case None =>
        Seq.empty[Node]
    }
  }
}
