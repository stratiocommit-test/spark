/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy.rest

import java.lang.Boolean

/**
 * An abstract response sent from the server in the REST application submission protocol.
 */
private[rest] abstract class SubmitRestProtocolResponse extends SubmitRestProtocolMessage {
  var serverSparkVersion: String = null
  var success: Boolean = null
  var unknownFields: Array[String] = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(serverSparkVersion, "serverSparkVersion")
  }
}

/**
 * A response to a [[CreateSubmissionRequest]] in the REST application submission protocol.
 */
private[spark] class CreateSubmissionResponse extends SubmitRestProtocolResponse {
  var submissionId: String = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(success, "success")
  }
}

/**
 * A response to a kill request in the REST application submission protocol.
 */
private[spark] class KillSubmissionResponse extends SubmitRestProtocolResponse {
  var submissionId: String = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(submissionId, "submissionId")
    assertFieldIsSet(success, "success")
  }
}

/**
 * A response to a status request in the REST application submission protocol.
 */
private[spark] class SubmissionStatusResponse extends SubmitRestProtocolResponse {
  var submissionId: String = null
  var driverState: String = null
  var workerId: String = null
  var workerHostPort: String = null

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(submissionId, "submissionId")
    assertFieldIsSet(success, "success")
  }
}

/**
 * An error response message used in the REST application submission protocol.
 */
private[rest] class ErrorResponse extends SubmitRestProtocolResponse {
  // The highest protocol version that the server knows about
  // This is set when the client specifies an unknown version
  var highestProtocolVersion: String = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(message, "message")
  }
}
