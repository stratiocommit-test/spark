/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.ui

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.internal.Logging
import org.apache.spark.ui.{UIUtils, WebUIPage}

class ExecutionPage(parent: SQLTab) extends WebUIPage("execution") with Logging {

  private val listener = parent.listener

  override def render(request: HttpServletRequest): Seq[Node] = listener.synchronized {
    val parameterExecutionId = request.getParameter("id")
    require(parameterExecutionId != null && parameterExecutionId.nonEmpty,
      "Missing execution id parameter")

    val executionId = parameterExecutionId.toLong
    val content = listener.getExecution(executionId).map { executionUIData =>
      val currentTime = System.currentTimeMillis()
      val duration =
        executionUIData.completionTime.getOrElse(currentTime) - executionUIData.submissionTime

      val summary =
        <div>
          <ul class="unstyled">
            <li>
              <strong>Submitted Time: </strong>{UIUtils.formatDate(executionUIData.submissionTime)}
            </li>
            <li>
              <strong>Duration: </strong>{UIUtils.formatDuration(duration)}
            </li>
            {if (executionUIData.runningJobs.nonEmpty) {
              <li>
                <strong>Running Jobs: </strong>
                {executionUIData.runningJobs.sorted.map { jobId =>
                <a href={jobURL(jobId)}>{jobId.toString}</a><span>&nbsp;</span>
              }}
              </li>
            }}
            {if (executionUIData.succeededJobs.nonEmpty) {
              <li>
                <strong>Succeeded Jobs: </strong>
                {executionUIData.succeededJobs.sorted.map { jobId =>
                  <a href={jobURL(jobId)}>{jobId.toString}</a><span>&nbsp;</span>
                }}
              </li>
            }}
            {if (executionUIData.failedJobs.nonEmpty) {
              <li>
                <strong>Failed Jobs: </strong>
                {executionUIData.failedJobs.sorted.map { jobId =>
                  <a href={jobURL(jobId)}>{jobId.toString}</a><span>&nbsp;</span>
                }}
              </li>
            }}
          </ul>
        </div>

      val metrics = listener.getExecutionMetrics(executionId)

      summary ++
        planVisualization(metrics, executionUIData.physicalPlanGraph) ++
        physicalPlanDescription(executionUIData.physicalPlanDescription)
    }.getOrElse {
      <div>No information to display for Plan {executionId}</div>
    }

    UIUtils.headerSparkPage(s"Details for Query $executionId", content, parent, Some(5000))
  }


  private def planVisualizationResources: Seq[Node] = {
    // scalastyle:off
    <link rel="stylesheet" href={UIUtils.prependBaseUri("/static/sql/spark-sql-viz.css")} type="text/css"/>
    <script src={UIUtils.prependBaseUri("/static/d3.min.js")}></script>
    <script src={UIUtils.prependBaseUri("/static/dagre-d3.min.js")}></script>
    <script src={UIUtils.prependBaseUri("/static/graphlib-dot.min.js")}></script>
    <script src={UIUtils.prependBaseUri("/static/sql/spark-sql-viz.js")}></script>
    // scalastyle:on
  }

  private def planVisualization(metrics: Map[Long, String], graph: SparkPlanGraph): Seq[Node] = {
    val metadata = graph.allNodes.flatMap { node =>
      val nodeId = s"plan-meta-data-${node.id}"
      <div id={nodeId}>{node.desc}</div>
    }

    <div>
      <div id="plan-viz-graph"></div>
      <div id="plan-viz-metadata" style="display:none">
        <div class="dot-file">
          {graph.makeDotFile(metrics)}
        </div>
        <div id="plan-viz-metadata-size">{graph.allNodes.size.toString}</div>
        {metadata}
      </div>
      {planVisualizationResources}
      <script>$(function() {{ renderPlanViz(); }})</script>
    </div>
  }

  private def jobURL(jobId: Long): String =
    "%s/jobs/job?id=%s".format(UIUtils.prependBaseUri(parent.basePath), jobId)

  private def physicalPlanDescription(physicalPlanDescription: String): Seq[Node] = {
    <div>
      <span style="cursor: pointer;" onclick="clickPhysicalPlanDetails();">
        <span id="physical-plan-details-arrow" class="arrow-closed"></span>
        <a>Details</a>
      </span>
    </div>
    <div id="physical-plan-details" style="display: none;">
      <pre>{physicalPlanDescription}</pre>
    </div>
    <script>
      function clickPhysicalPlanDetails() {{
        $('#physical-plan-details').toggle();
        $('#physical-plan-details-arrow').toggleClass('arrow-open').toggleClass('arrow-closed');
      }}
    </script>
    <br/>
  }
}
