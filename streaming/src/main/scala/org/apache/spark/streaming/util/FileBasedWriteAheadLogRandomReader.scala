/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.util

import java.io.Closeable
import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration

/**
 * A random access reader for reading write ahead log files written using
 * [[org.apache.spark.streaming.util.FileBasedWriteAheadLogWriter]]. Given the file segment info,
 * this reads the record (ByteBuffer) from the log file.
 */
private[streaming] class FileBasedWriteAheadLogRandomReader(path: String, conf: Configuration)
  extends Closeable {

  private val instream = HdfsUtils.getInputStream(path, conf)
  private var closed = (instream == null) // the file may be deleted as we're opening the stream

  def read(segment: FileBasedWriteAheadLogSegment): ByteBuffer = synchronized {
    assertOpen()
    instream.seek(segment.offset)
    val nextLength = instream.readInt()
    HdfsUtils.checkState(nextLength == segment.length,
      s"Expected message length to be ${segment.length}, but was $nextLength")
    val buffer = new Array[Byte](nextLength)
    instream.readFully(buffer)
    ByteBuffer.wrap(buffer)
  }

  override def close(): Unit = synchronized {
    closed = true
    instream.close()
  }

  private def assertOpen() {
    HdfsUtils.checkState(!closed, "Stream is closed. Create a new Reader to read from the file.")
  }
}
