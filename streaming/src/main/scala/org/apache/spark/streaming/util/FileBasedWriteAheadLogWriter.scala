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

import java.io._
import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration

import org.apache.spark.util.Utils

/**
 * A writer for writing byte-buffers to a write ahead log file.
 */
private[streaming] class FileBasedWriteAheadLogWriter(path: String, hadoopConf: Configuration)
  extends Closeable {

  private lazy val stream = HdfsUtils.getOutputStream(path, hadoopConf)


  private var nextOffset = stream.getPos()
  private var closed = false

  /** Write the bytebuffer to the log file */
  def write(data: ByteBuffer): FileBasedWriteAheadLogSegment = synchronized {
    assertOpen()
    data.rewind() // Rewind to ensure all data in the buffer is retrieved
    val lengthToWrite = data.remaining()
    val segment = new FileBasedWriteAheadLogSegment(path, nextOffset, lengthToWrite)
    stream.writeInt(lengthToWrite)
    Utils.writeByteBuffer(data, stream: OutputStream)
    flush()
    nextOffset = stream.getPos()
    segment
  }

  override def close(): Unit = synchronized {
    closed = true
    stream.close()
  }

  private def flush() {
    stream.hflush()
    // Useful for local file system where hflush/sync does not work (HADOOP-7844)
    stream.getWrappedStream.flush()
  }

  private def assertOpen() {
    HdfsUtils.checkState(!closed, "Stream is closed. Create a new Writer to write to file.")
  }
}
