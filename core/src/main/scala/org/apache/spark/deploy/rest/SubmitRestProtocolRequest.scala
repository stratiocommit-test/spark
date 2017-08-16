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

import scala.util.Try

import org.apache.spark.util.Utils

/**
 * An abstract request sent from the client in the REST application submission protocol.
 */
private[rest] abstract class SubmitRestProtocolRequest extends SubmitRestProtocolMessage {
  var clientSparkVersion: String = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(clientSparkVersion, "clientSparkVersion")
  }
}

/**
 * A request to launch a new application in the REST application submission protocol.
 */
private[rest] class CreateSubmissionRequest extends SubmitRestProtocolRequest {
  var appResource: String = null
  var mainClass: String = null
  var appArgs: Array[String] = null
  var sparkProperties: Map[String, String] = null
  var environmentVariables: Map[String, String] = null

  protected override def doValidate(): Unit = {
    super.doValidate()
    assert(sparkProperties != null, "No Spark properties set!")
    assertFieldIsSet(appResource, "appResource")
    assertPropertyIsSet("spark.app.name")
    assertPropertyIsBoolean("spark.driver.supervise")
    assertPropertyIsNumeric("spark.driver.cores")
    assertPropertyIsNumeric("spark.cores.max")
    assertPropertyIsMemory("spark.driver.memory")
    assertPropertyIsMemory("spark.executor.memory")
  }

  private def assertPropertyIsSet(key: String): Unit =
    assertFieldIsSet(sparkProperties.getOrElse(key, null), key)

  private def assertPropertyIsBoolean(key: String): Unit =
    assertProperty[Boolean](key, "boolean", _.toBoolean)

  private def assertPropertyIsNumeric(key: String): Unit =
    assertProperty[Double](key, "numeric", _.toDouble)

  private def assertPropertyIsMemory(key: String): Unit =
    assertProperty[Int](key, "memory", Utils.memoryStringToMb)

  /** Assert that a Spark property can be converted to a certain type. */
  private def assertProperty[T](key: String, valueType: String, convert: (String => T)): Unit = {
    sparkProperties.get(key).foreach { value =>
      Try(convert(value)).getOrElse {
        throw new SubmitRestProtocolException(
          s"Property '$key' expected $valueType value: actual was '$value'.")
      }
    }
  }
}
