/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark

import java.io.{FileDescriptor, InputStream}
import java.lang
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.fs._

import org.apache.spark.internal.Logging

object DebugFilesystem extends Logging {
  // Stores the set of active streams and their creation sites.
  private val openStreams = new ConcurrentHashMap[FSDataInputStream, Throwable]()

  def clearOpenStreams(): Unit = {
    openStreams.clear()
  }

  def assertNoOpenStreams(): Unit = {
    val numOpen = openStreams.size()
    if (numOpen > 0) {
      for (exc <- openStreams.values().asScala) {
        logWarning("Leaked filesystem connection created at:")
        exc.printStackTrace()
      }
      throw new RuntimeException(s"There are $numOpen possibly leaked file streams.")
    }
  }
}

/**
 * DebugFilesystem wraps file open calls to track all open connections. This can be used in tests
 * to check that connections are not leaked.
 */
// TODO(ekl) we should consider always interposing this to expose num open conns as a metric
class DebugFilesystem extends LocalFileSystem {
  import DebugFilesystem._

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    val wrapped: FSDataInputStream = super.open(f, bufferSize)
    openStreams.put(wrapped, new Throwable())

    new FSDataInputStream(wrapped.getWrappedStream) {
      override def setDropBehind(dropBehind: lang.Boolean): Unit = wrapped.setDropBehind(dropBehind)

      override def getWrappedStream: InputStream = wrapped.getWrappedStream

      override def getFileDescriptor: FileDescriptor = wrapped.getFileDescriptor

      override def getPos: Long = wrapped.getPos

      override def seekToNewSource(targetPos: Long): Boolean = wrapped.seekToNewSource(targetPos)

      override def seek(desired: Long): Unit = wrapped.seek(desired)

      override def setReadahead(readahead: lang.Long): Unit = wrapped.setReadahead(readahead)

      override def read(position: Long, buffer: Array[Byte], offset: Int, length: Int): Int =
        wrapped.read(position, buffer, offset, length)

      override def read(buf: ByteBuffer): Int = wrapped.read(buf)

      override def readFully(position: Long, buffer: Array[Byte], offset: Int, length: Int): Unit =
        wrapped.readFully(position, buffer, offset, length)

      override def readFully(position: Long, buffer: Array[Byte]): Unit =
        wrapped.readFully(position, buffer)

      override def available(): Int = wrapped.available()

      override def mark(readlimit: Int): Unit = wrapped.mark(readlimit)

      override def skip(n: Long): Long = wrapped.skip(n)

      override def markSupported(): Boolean = wrapped.markSupported()

      override def close(): Unit = {
        wrapped.close()
        openStreams.remove(wrapped)
      }

      override def read(): Int = wrapped.read()

      override def reset(): Unit = wrapped.reset()

      override def toString: String = wrapped.toString

      override def equals(obj: scala.Any): Boolean = wrapped.equals(obj)

      override def hashCode(): Int = wrapped.hashCode()
    }
  }
}
