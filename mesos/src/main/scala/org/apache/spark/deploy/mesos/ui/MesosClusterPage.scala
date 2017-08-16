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

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.mesos.Protos.TaskStatus

import org.apache.spark.deploy.mesos.config._
import org.apache.spark.deploy.mesos.MesosDriverDescription
import org.apache.spark.scheduler.cluster.mesos.MesosClusterSubmissionState
import org.apache.spark.ui.{UIUtils, WebUIPage}

private[mesos] class MesosClusterPage(parent: MesosClusterUI) extends WebUIPage("") {
  private val historyServerURL = parent.conf.get(HISTORY_SERVER_URL)

  def render(request: HttpServletRequest): Seq[Node] = {
    val state = parent.scheduler.getSchedulerState()

    val driverHeader = Seq("Driver ID")
    val historyHeader = historyServerURL.map(url => Seq("History")).getOrElse(Nil)
    val submissionHeader = Seq("Submit Date", "Main Class", "Driver Resources")

    val queuedHeaders = driverHeader ++ submissionHeader
    val driverHeaders = driverHeader ++ historyHeader ++ submissionHeader ++
      Seq("Start Date", "Mesos Slave ID", "State")
    val retryHeaders = Seq("Driver ID", "Submit Date", "Description") ++
      Seq("Last Failed Status", "Next Retry Time", "Attempt Count")
    val queuedTable = UIUtils.listingTable(queuedHeaders, queuedRow, state.queuedDrivers)
    val launchedTable = UIUtils.listingTable(driverHeaders, driverRow, state.launchedDrivers)
    val finishedTable = UIUtils.listingTable(driverHeaders, driverRow, state.finishedDrivers)
    val retryTable = UIUtils.listingTable(retryHeaders, retryRow, state.pendingRetryDrivers)
    val content =
      <p>Mesos Framework ID: {state.frameworkId}</p>
      <div class="row-fluid">
        <div class="span12">
          <h4>Queued Drivers:</h4>
          {queuedTable}
          <h4>Launched Drivers:</h4>
          {launchedTable}
          <h4>Finished Drivers:</h4>
          {finishedTable}
          <h4>Supervise drivers waiting for retry:</h4>
          {retryTable}
        </div>
      </div>;
    UIUtils.basicSparkPage(content, "Spark Drivers for Mesos cluster")
  }

  private def queuedRow(submission: MesosDriverDescription): Seq[Node] = {
    val id = submission.submissionId
    <tr>
      <td><a href={s"driver?id=$id"}>{id}</a></td>
      <td>{submission.submissionDate}</td>
      <td>{submission.command.mainClass}</td>
      <td>cpus: {submission.cores}, mem: {submission.mem}</td>
    </tr>
  }

  private def driverRow(state: MesosClusterSubmissionState): Seq[Node] = {
    val id = state.driverDescription.submissionId

    val historyCol = if (historyServerURL.isDefined) {
      <td>
        <a href={s"${historyServerURL.get}/history/${state.frameworkId}"}>
          {state.frameworkId}
        </a>
      </td>
    } else Nil

    <tr>
      <td><a href={s"driver?id=$id"}>{id}</a></td>
      {historyCol}
      <td>{state.driverDescription.submissionDate}</td>
      <td>{state.driverDescription.command.mainClass}</td>
      <td>cpus: {state.driverDescription.cores}, mem: {state.driverDescription.mem}</td>
      <td>{state.startDate}</td>
      <td>{state.slaveId.getValue}</td>
      <td>{stateString(state.mesosTaskStatus)}</td>
    </tr>
  }

  private def retryRow(submission: MesosDriverDescription): Seq[Node] = {
    val id = submission.submissionId
    <tr>
      <td><a href={s"driver?id=$id"}>{id}</a></td>
      <td>{submission.submissionDate}</td>
      <td>{submission.command.mainClass}</td>
      <td>{submission.retryState.get.lastFailureStatus}</td>
      <td>{submission.retryState.get.nextRetry}</td>
      <td>{submission.retryState.get.retries}</td>
    </tr>
  }

  private def stateString(status: Option[TaskStatus]): String = {
    if (status.isEmpty) {
      return ""
    }
    val sb = new StringBuilder
    val s = status.get
    sb.append(s"State: ${s.getState}")
    if (status.get.hasMessage) {
      sb.append(s", Message: ${s.getMessage}")
    }
    if (status.get.hasHealthy) {
      sb.append(s", Healthy: ${s.getHealthy}")
    }
    if (status.get.hasSource) {
      sb.append(s", Source: ${s.getSource}")
    }
    if (status.get.hasReason) {
      sb.append(s", Reason: ${s.getReason}")
    }
    if (status.get.hasTimestamp) {
      sb.append(s", Time: ${s.getTimestamp}")
    }
    sb.toString()
  }
}
