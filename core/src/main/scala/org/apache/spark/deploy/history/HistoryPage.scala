/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy.history

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.ui.{UIUtils, WebUIPage}

private[history] class HistoryPage(parent: HistoryServer) extends WebUIPage("") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val requestedIncomplete =
      Option(request.getParameter("showIncomplete")).getOrElse("false").toBoolean

    val allAppsSize = parent.getApplicationList().count(_.completed != requestedIncomplete)
    val eventLogsUnderProcessCount = parent.getEventLogsUnderProcess()
    val lastUpdatedTime = parent.getLastUpdatedTime()
    val providerConfig = parent.getProviderConfig()
    val content =
      <script src={UIUtils.prependBaseUri("/static/historypage-common.js")}></script>
      <div>
          <div class="span12">
            <ul class="unstyled">
              {providerConfig.map { case (k, v) => <li><strong>{k}:</strong> {v}</li> }}
            </ul>
            {
            if (eventLogsUnderProcessCount > 0) {
              <p>There are {eventLogsUnderProcessCount} event log(s) currently being
                processed which may result in additional applications getting listed on this page.
                Refresh the page to view updates. </p>
            }
            }

            {
            if (lastUpdatedTime > 0) {
              <p>Last updated: <span id="last-updated">{lastUpdatedTime}</span></p>
            }
            }

            {
            if (allAppsSize > 0) {
              <script src={UIUtils.prependBaseUri("/static/dataTables.rowsGroup.js")}></script> ++
                <div id="history-summary" class="span12 pagination"></div> ++
                <script src={UIUtils.prependBaseUri("/static/utils.js")}></script> ++
                <script src={UIUtils.prependBaseUri("/static/historypage.js")}></script> ++
                <script>setAppLimit({parent.maxApplications})</script>
            } else if (requestedIncomplete) {
              <h4>No incomplete applications found!</h4>
            } else if (eventLogsUnderProcessCount > 0) {
              <h4>No completed applications found!</h4>
            } else {
              <h4>No completed applications found!</h4> ++ parent.emptyListingHtml
            }
            }

            <a href={makePageLink(!requestedIncomplete)}>
              {
              if (requestedIncomplete) {
                "Back to completed applications"
              } else {
                "Show incomplete applications"
              }
              }
            </a>
          </div>
      </div>
    UIUtils.basicSparkPage(content, "History Server", true)
  }

  private def makePageLink(showIncomplete: Boolean): String = {
    UIUtils.prependBaseUri("/?" + "showIncomplete=" + showIncomplete)
  }
}
