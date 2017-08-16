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

import java.io.OutputStream
import java.util.zip.ZipOutputStream
import javax.ws.rs.{GET, Produces}
import javax.ws.rs.core.{MediaType, Response, StreamingOutput}

import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging

@Produces(Array(MediaType.APPLICATION_OCTET_STREAM))
private[v1] class EventLogDownloadResource(
    val uIRoot: UIRoot,
    val appId: String,
    val attemptId: Option[String]) extends Logging {
  val conf = SparkHadoopUtil.get.newConfiguration(new SparkConf)

  @GET
  def getEventLogs(): Response = {
    try {
      val fileName = {
        attemptId match {
          case Some(id) => s"eventLogs-$appId-$id.zip"
          case None => s"eventLogs-$appId.zip"
        }
      }

      val stream = new StreamingOutput {
        override def write(output: OutputStream): Unit = {
          val zipStream = new ZipOutputStream(output)
          try {
            uIRoot.writeEventLogs(appId, attemptId, zipStream)
          } finally {
            zipStream.close()
          }

        }
      }

      Response.ok(stream)
        .header("Content-Disposition", s"attachment; filename=$fileName")
        .header("Content-Type", MediaType.APPLICATION_OCTET_STREAM)
        .build()
    } catch {
      case NonFatal(e) =>
        Response.serverError()
          .entity(s"Event logs are not available for app: $appId.")
          .status(Response.Status.SERVICE_UNAVAILABLE)
          .build()
    }
  }
}
