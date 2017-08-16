/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.scheduler.cluster

import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, ApplicationId}

/**
 * A stub application ID; can be set in constructor and/or updated later.
 * @param applicationId application ID
 * @param attempt an attempt counter
 */
class StubApplicationAttemptId(var applicationId: ApplicationId, var attempt: Int)
    extends ApplicationAttemptId {

  override def setApplicationId(appID: ApplicationId): Unit = {
    applicationId = appID
  }

  override def getAttemptId: Int = {
    attempt
  }

  override def setAttemptId(attemptId: Int): Unit = {
    attempt = attemptId
  }

  override def getApplicationId: ApplicationId = {
    applicationId
  }

  override def build(): Unit = {
  }
}
