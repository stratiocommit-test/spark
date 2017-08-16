/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.streaming

import scala.util.control.Exception._

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

/**
 * Offset for the [[FileStreamSource]].
 * @param logOffset  Position in the [[FileStreamSourceLog]]
 */
case class FileStreamSourceOffset(logOffset: Long) extends Offset {
  override def json: String = {
    Serialization.write(this)(FileStreamSourceOffset.format)
  }
}

object FileStreamSourceOffset {
  implicit val format = Serialization.formats(NoTypeHints)

  def apply(offset: Offset): FileStreamSourceOffset = {
    offset match {
      case f: FileStreamSourceOffset => f
      case SerializedOffset(str) =>
        catching(classOf[NumberFormatException]).opt {
          FileStreamSourceOffset(str.toLong)
        }.getOrElse {
          Serialization.read[FileStreamSourceOffset](str)
        }
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to FileStreamSourceOffset")
    }
  }
}

