/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark

import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.language.postfixOps

import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually._

import org.apache.spark.JobExecutionStatus._

class StatusTrackerSuite extends SparkFunSuite with Matchers with LocalSparkContext {

  test("basic status API usage") {
    sc = new SparkContext("local", "test", new SparkConf(false))
    val jobFuture = sc.parallelize(1 to 10000, 2).map(identity).groupBy(identity).collectAsync()
    val jobId: Int = eventually(timeout(10 seconds)) {
      val jobIds = jobFuture.jobIds
      jobIds.size should be(1)
      jobIds.head
    }
    val jobInfo = eventually(timeout(10 seconds)) {
      sc.statusTracker.getJobInfo(jobId).get
    }
    jobInfo.status() should not be FAILED
    val stageIds = jobInfo.stageIds()
    stageIds.size should be(2)

    val firstStageInfo = eventually(timeout(10 seconds)) {
      sc.statusTracker.getStageInfo(stageIds(0)).get
    }
    firstStageInfo.stageId() should be(stageIds(0))
    firstStageInfo.currentAttemptId() should be(0)
    firstStageInfo.numTasks() should be(2)
    eventually(timeout(10 seconds)) {
      val updatedFirstStageInfo = sc.statusTracker.getStageInfo(stageIds(0)).get
      updatedFirstStageInfo.numCompletedTasks() should be(2)
      updatedFirstStageInfo.numActiveTasks() should be(0)
      updatedFirstStageInfo.numFailedTasks() should be(0)
    }
  }

  test("getJobIdsForGroup()") {
    sc = new SparkContext("local", "test", new SparkConf(false))
    // Passing `null` should return jobs that were not run in a job group:
    val defaultJobGroupFuture = sc.parallelize(1 to 1000).countAsync()
    val defaultJobGroupJobId = eventually(timeout(10 seconds)) {
      defaultJobGroupFuture.jobIds.head
    }
    eventually(timeout(10 seconds)) {
      sc.statusTracker.getJobIdsForGroup(null).toSet should be (Set(defaultJobGroupJobId))
    }
    // Test jobs submitted in job groups:
    sc.setJobGroup("my-job-group", "description")
    sc.statusTracker.getJobIdsForGroup("my-job-group") should be (Seq.empty)
    val firstJobFuture = sc.parallelize(1 to 1000).countAsync()
    val firstJobId = eventually(timeout(10 seconds)) {
      firstJobFuture.jobIds.head
    }
    eventually(timeout(10 seconds)) {
      sc.statusTracker.getJobIdsForGroup("my-job-group") should be (Seq(firstJobId))
    }
    val secondJobFuture = sc.parallelize(1 to 1000).countAsync()
    val secondJobId = eventually(timeout(10 seconds)) {
      secondJobFuture.jobIds.head
    }
    eventually(timeout(10 seconds)) {
      sc.statusTracker.getJobIdsForGroup("my-job-group").toSet should be (
        Set(firstJobId, secondJobId))
    }
  }

  test("getJobIdsForGroup() with takeAsync()") {
    sc = new SparkContext("local", "test", new SparkConf(false))
    sc.setJobGroup("my-job-group2", "description")
    sc.statusTracker.getJobIdsForGroup("my-job-group2") shouldBe empty
    val firstJobFuture = sc.parallelize(1 to 1000, 1).takeAsync(1)
    val firstJobId = eventually(timeout(10 seconds)) {
      firstJobFuture.jobIds.head
    }
    eventually(timeout(10 seconds)) {
      sc.statusTracker.getJobIdsForGroup("my-job-group2") should be (Seq(firstJobId))
    }
  }

  test("getJobIdsForGroup() with takeAsync() across multiple partitions") {
    sc = new SparkContext("local", "test", new SparkConf(false))
    sc.setJobGroup("my-job-group2", "description")
    sc.statusTracker.getJobIdsForGroup("my-job-group2") shouldBe empty
    val firstJobFuture = sc.parallelize(1 to 1000, 2).takeAsync(999)
    val firstJobId = eventually(timeout(10 seconds)) {
      firstJobFuture.jobIds.head
    }
    eventually(timeout(10 seconds)) {
      sc.statusTracker.getJobIdsForGroup("my-job-group2") should have size 2
    }
  }
}
