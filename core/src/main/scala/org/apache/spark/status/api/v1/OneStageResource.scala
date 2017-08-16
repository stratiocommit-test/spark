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

import javax.ws.rs._
import javax.ws.rs.core.MediaType

import org.apache.spark.SparkException
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.status.api.v1.StageStatus._
import org.apache.spark.status.api.v1.TaskSorting._
import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.jobs.JobProgressListener
import org.apache.spark.ui.jobs.UIData.StageUIData

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class OneStageResource(ui: SparkUI) {

  @GET
  @Path("")
  def stageData(@PathParam("stageId") stageId: Int): Seq[StageData] = {
    withStage(stageId) { stageAttempts =>
      stageAttempts.map { stage =>
        AllStagesResource.stageUiToStageData(stage.status, stage.info, stage.ui,
          includeDetails = true)
      }
    }
  }

  @GET
  @Path("/{stageAttemptId: \\d+}")
  def oneAttemptData(
      @PathParam("stageId") stageId: Int,
      @PathParam("stageAttemptId") stageAttemptId: Int): StageData = {
    withStageAttempt(stageId, stageAttemptId) { stage =>
      AllStagesResource.stageUiToStageData(stage.status, stage.info, stage.ui,
        includeDetails = true)
    }
  }

  @GET
  @Path("/{stageAttemptId: \\d+}/taskSummary")
  def taskSummary(
      @PathParam("stageId") stageId: Int,
      @PathParam("stageAttemptId") stageAttemptId: Int,
      @DefaultValue("0.05,0.25,0.5,0.75,0.95") @QueryParam("quantiles") quantileString: String)
  : TaskMetricDistributions = {
    withStageAttempt(stageId, stageAttemptId) { stage =>
      val quantiles = quantileString.split(",").map { s =>
        try {
          s.toDouble
        } catch {
          case nfe: NumberFormatException =>
            throw new BadParameterException("quantiles", "double", s)
        }
      }
      AllStagesResource.taskMetricDistributions(stage.ui.taskData.values, quantiles)
    }
  }

  @GET
  @Path("/{stageAttemptId: \\d+}/taskList")
  def taskList(
      @PathParam("stageId") stageId: Int,
      @PathParam("stageAttemptId") stageAttemptId: Int,
      @DefaultValue("0") @QueryParam("offset") offset: Int,
      @DefaultValue("20") @QueryParam("length") length: Int,
      @DefaultValue("ID") @QueryParam("sortBy") sortBy: TaskSorting): Seq[TaskData] = {
    withStageAttempt(stageId, stageAttemptId) { stage =>
      val tasks = stage.ui.taskData.values.map{AllStagesResource.convertTaskData}.toIndexedSeq
        .sorted(OneStageResource.ordering(sortBy))
      tasks.slice(offset, offset + length)
    }
  }

  private case class StageStatusInfoUi(status: StageStatus, info: StageInfo, ui: StageUIData)

  private def withStage[T](stageId: Int)(f: Seq[StageStatusInfoUi] => T): T = {
    val stageAttempts = findStageStatusUIData(ui.jobProgressListener, stageId)
    if (stageAttempts.isEmpty) {
      throw new NotFoundException("unknown stage: " + stageId)
    } else {
      f(stageAttempts)
    }
  }

  private def findStageStatusUIData(
      listener: JobProgressListener,
      stageId: Int): Seq[StageStatusInfoUi] = {
    listener.synchronized {
      def getStatusInfoUi(status: StageStatus, infos: Seq[StageInfo]): Seq[StageStatusInfoUi] = {
        infos.filter { _.stageId == stageId }.map { info =>
          val ui = listener.stageIdToData.getOrElse((info.stageId, info.attemptId),
            // this is an internal error -- we should always have uiData
            throw new SparkException(
              s"no stage ui data found for stage: ${info.stageId}:${info.attemptId}")
          )
          StageStatusInfoUi(status, info, ui)
        }
      }
      getStatusInfoUi(ACTIVE, listener.activeStages.values.toSeq) ++
        getStatusInfoUi(COMPLETE, listener.completedStages) ++
        getStatusInfoUi(FAILED, listener.failedStages) ++
        getStatusInfoUi(PENDING, listener.pendingStages.values.toSeq)
    }
  }

  private def withStageAttempt[T](
      stageId: Int,
      stageAttemptId: Int)
      (f: StageStatusInfoUi => T): T = {
    withStage(stageId) { attempts =>
        val oneAttempt = attempts.find { stage => stage.info.attemptId == stageAttemptId }
        oneAttempt match {
          case Some(stage) =>
            f(stage)
          case None =>
            val stageAttempts = attempts.map { _.info.attemptId }
            throw new NotFoundException(s"unknown attempt for stage $stageId.  " +
              s"Found attempts: ${stageAttempts.mkString("[", ",", "]")}")
        }
    }
  }
}

object OneStageResource {
  def ordering(taskSorting: TaskSorting): Ordering[TaskData] = {
    val extractor: (TaskData => Long) = td =>
      taskSorting match {
        case ID => td.taskId
        case INCREASING_RUNTIME => td.taskMetrics.map{_.executorRunTime}.getOrElse(-1L)
        case DECREASING_RUNTIME => -td.taskMetrics.map{_.executorRunTime}.getOrElse(-1L)
      }
    Ordering.by(extractor)
  }
}
