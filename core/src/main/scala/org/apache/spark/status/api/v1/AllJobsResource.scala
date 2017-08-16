/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.status.api.v1

import java.util.{Arrays, Date, List => JList}
import javax.ws.rs._
import javax.ws.rs.core.MediaType

import org.apache.spark.JobExecutionStatus
import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.jobs.JobProgressListener
import org.apache.spark.ui.jobs.UIData.JobUIData

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class AllJobsResource(ui: SparkUI) {

  @GET
  def jobsList(@QueryParam("status") statuses: JList[JobExecutionStatus]): Seq[JobData] = {
    val statusToJobs: Seq[(JobExecutionStatus, Seq[JobUIData])] =
      AllJobsResource.getStatusToJobs(ui)
    val adjStatuses: JList[JobExecutionStatus] = {
      if (statuses.isEmpty) {
        Arrays.asList(JobExecutionStatus.values(): _*)
      } else {
        statuses
      }
    }
    val jobInfos = for {
      (status, jobs) <- statusToJobs
      job <- jobs if adjStatuses.contains(status)
    } yield {
      AllJobsResource.convertJobData(job, ui.jobProgressListener, false)
    }
    jobInfos.sortBy{- _.jobId}
  }

}

private[v1] object AllJobsResource {

  def getStatusToJobs(ui: SparkUI): Seq[(JobExecutionStatus, Seq[JobUIData])] = {
    val statusToJobs = ui.jobProgressListener.synchronized {
      Seq(
        JobExecutionStatus.RUNNING -> ui.jobProgressListener.activeJobs.values.toSeq,
        JobExecutionStatus.SUCCEEDED -> ui.jobProgressListener.completedJobs.toSeq,
        JobExecutionStatus.FAILED -> ui.jobProgressListener.failedJobs.reverse.toSeq
      )
    }
    statusToJobs
  }

  def convertJobData(
      job: JobUIData,
      listener: JobProgressListener,
      includeStageDetails: Boolean): JobData = {
    listener.synchronized {
      val lastStageInfo =
        if (job.stageIds.isEmpty) {
          None
        } else {
          listener.stageIdToInfo.get(job.stageIds.max)
        }
      val lastStageData = lastStageInfo.flatMap { s =>
        listener.stageIdToData.get((s.stageId, s.attemptId))
      }
      val lastStageName = lastStageInfo.map { _.name }.getOrElse("(Unknown Stage Name)")
      val lastStageDescription = lastStageData.flatMap { _.description }
      new JobData(
        jobId = job.jobId,
        name = lastStageName,
        description = lastStageDescription,
        submissionTime = job.submissionTime.map{new Date(_)},
        completionTime = job.completionTime.map{new Date(_)},
        stageIds = job.stageIds,
        jobGroup = job.jobGroup,
        status = job.status,
        numTasks = job.numTasks,
        numActiveTasks = job.numActiveTasks,
        numCompletedTasks = job.numCompletedTasks,
        numSkippedTasks = job.numSkippedTasks,
        numFailedTasks = job.numFailedTasks,
        numActiveStages = job.numActiveStages,
        numCompletedStages = job.completedStageIndices.size,
        numSkippedStages = job.numSkippedStages,
        numFailedStages = job.numFailedStages
      )
    }
  }
}
