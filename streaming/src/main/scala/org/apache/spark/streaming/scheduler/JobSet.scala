/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.scheduler

import scala.collection.mutable.HashSet

import org.apache.spark.streaming.Time

/**
 * Class representing a set of Jobs
 * belong to the same batch.
 */
private[streaming]
case class JobSet(
    time: Time,
    jobs: Seq[Job],
    streamIdToInputInfo: Map[Int, StreamInputInfo] = Map.empty) {

  private val incompleteJobs = new HashSet[Job]()
  private val submissionTime = System.currentTimeMillis() // when this jobset was submitted
  private var processingStartTime = -1L // when the first job of this jobset started processing
  private var processingEndTime = -1L // when the last job of this jobset finished processing

  jobs.zipWithIndex.foreach { case (job, i) => job.setOutputOpId(i) }
  incompleteJobs ++= jobs

  def handleJobStart(job: Job) {
    if (processingStartTime < 0) processingStartTime = System.currentTimeMillis()
  }

  def handleJobCompletion(job: Job) {
    incompleteJobs -= job
    if (hasCompleted) processingEndTime = System.currentTimeMillis()
  }

  def hasStarted: Boolean = processingStartTime > 0

  def hasCompleted: Boolean = incompleteJobs.isEmpty

  // Time taken to process all the jobs from the time they started processing
  // (i.e. not including the time they wait in the streaming scheduler queue)
  def processingDelay: Long = processingEndTime - processingStartTime

  // Time taken to process all the jobs from the time they were submitted
  // (i.e. including the time they wait in the streaming scheduler queue)
  def totalDelay: Long = processingEndTime - time.milliseconds

  def toBatchInfo: BatchInfo = {
    BatchInfo(
      time,
      streamIdToInputInfo,
      submissionTime,
      if (hasStarted) Some(processingStartTime) else None,
      if (hasCompleted) Some(processingEndTime) else None,
      jobs.map { job => (job.outputOpId, job.toOutputOperationInfo) }.toMap
    )
  }
}
