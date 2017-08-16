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

import javax.ws.rs.{GET, PathParam, Produces}
import javax.ws.rs.core.MediaType

import org.apache.spark.JobExecutionStatus
import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.jobs.UIData.JobUIData

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class OneJobResource(ui: SparkUI) {

  @GET
  def oneJob(@PathParam("jobId") jobId: Int): JobData = {
    val statusToJobs: Seq[(JobExecutionStatus, Seq[JobUIData])] =
      AllJobsResource.getStatusToJobs(ui)
    val jobOpt = statusToJobs.flatMap(_._2).find { jobInfo => jobInfo.jobId == jobId}
    jobOpt.map { job =>
      AllJobsResource.convertJobData(job, ui.jobProgressListener, false)
    }.getOrElse {
      throw new NotFoundException("unknown job: " + jobId)
    }
  }

}
