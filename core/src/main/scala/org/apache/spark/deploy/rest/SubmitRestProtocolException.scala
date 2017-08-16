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

/**
 * An exception thrown in the REST application submission protocol.
 */
private[rest] class SubmitRestProtocolException(message: String, cause: Throwable = null)
  extends Exception(message, cause)

/**
 * An exception thrown if a field is missing from a [[SubmitRestProtocolMessage]].
 */
private[rest] class SubmitRestMissingFieldException(message: String)
  extends SubmitRestProtocolException(message)

/**
 * An exception thrown if the REST client cannot reach the REST server.
 */
private[deploy] class SubmitRestConnectionException(message: String, cause: Throwable)
  extends SubmitRestProtocolException(message, cause)
