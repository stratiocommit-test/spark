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

import java.io.{InputStreamReader, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import scala.util.control.NonFatal

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, Path}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQuery

/**
 * Contains metadata associated with a [[StreamingQuery]]. This information is written
 * in the checkpoint location the first time a query is started and recovered every time the query
 * is restarted.
 *
 * @param id  unique id of the [[StreamingQuery]] that needs to be persisted across restarts
 */
case class StreamMetadata(id: String) {
  def json: String = Serialization.write(this)(StreamMetadata.format)
}

object StreamMetadata extends Logging {
  implicit val format = Serialization.formats(NoTypeHints)

  /** Read the metadata from file if it exists */
  def read(metadataFile: Path, hadoopConf: Configuration): Option[StreamMetadata] = {
    val fs = FileSystem.get(hadoopConf)
    if (fs.exists(metadataFile)) {
      var input: FSDataInputStream = null
      try {
        input = fs.open(metadataFile)
        val reader = new InputStreamReader(input, StandardCharsets.UTF_8)
        val metadata = Serialization.read[StreamMetadata](reader)
        Some(metadata)
      } catch {
        case NonFatal(e) =>
          logError(s"Error reading stream metadata from $metadataFile", e)
          throw e
      } finally {
        IOUtils.closeQuietly(input)
      }
    } else None
  }

  /** Write metadata to file */
  def write(
      metadata: StreamMetadata,
      metadataFile: Path,
      hadoopConf: Configuration): Unit = {
    var output: FSDataOutputStream = null
    try {
      val fs = FileSystem.get(hadoopConf)
      output = fs.create(metadataFile)
      val writer = new OutputStreamWriter(output)
      Serialization.write(metadata, writer)
      writer.close()
    } catch {
      case NonFatal(e) =>
        logError(s"Error writing stream metadata $metadata to $metadataFile", e)
        throw e
    } finally {
      IOUtils.closeQuietly(output)
    }
  }
}
