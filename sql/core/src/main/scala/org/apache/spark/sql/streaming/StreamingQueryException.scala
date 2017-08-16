/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.streaming

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.execution.streaming.{Offset, OffsetSeq, StreamExecution}

/**
 * :: Experimental ::
 * Exception that stopped a [[StreamingQuery]]. Use `cause` get the actual exception
 * that caused the failure.
 * @param message     Message of this exception
 * @param cause       Internal cause of this exception
 * @param startOffset Starting offset in json of the range of data in which exception occurred
 * @param endOffset   Ending offset in json of the range of data in exception occurred
 * @since 2.0.0
 */
@Experimental
class StreamingQueryException private(
    causeString: String,
    val message: String,
    val cause: Throwable,
    val startOffset: String,
    val endOffset: String)
  extends Exception(message, cause) {

  private[sql] def this(
      query: StreamingQuery,
      message: String,
      cause: Throwable,
      startOffset: String,
      endOffset: String) {
    this(
      // scalastyle:off
      s"""${classOf[StreamingQueryException].getName}: ${cause.getMessage} ${cause.getStackTrace.take(10).mkString("", "\n|\t", "\n")}
         |
         |${query.asInstanceOf[StreamExecution].toDebugString}
         """.stripMargin,
      // scalastyle:on
      message,
      cause,
      startOffset,
      endOffset)
  }

  /** Time when the exception occurred */
  val time: Long = System.currentTimeMillis

  override def toString(): String = causeString
}
