/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.dstream

import java.io.EOFException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{ReadableByteChannel, SocketChannel}
import java.util.concurrent.ArrayBlockingQueue

import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver.Receiver

/**
 * An input stream that reads blocks of serialized objects from a given network address.
 * The blocks will be inserted directly into the block store. This is the fastest way to get
 * data into Spark Streaming, though it requires the sender to batch data and serialize it
 * in the format that the system is configured with.
 */
private[streaming]
class RawInputDStream[T: ClassTag](
    _ssc: StreamingContext,
    host: String,
    port: Int,
    storageLevel: StorageLevel
  ) extends ReceiverInputDStream[T](_ssc) with Logging {

  def getReceiver(): Receiver[T] = {
    new RawNetworkReceiver(host, port, storageLevel).asInstanceOf[Receiver[T]]
  }
}

private[streaming]
class RawNetworkReceiver(host: String, port: Int, storageLevel: StorageLevel)
  extends Receiver[Any](storageLevel) with Logging {

  var blockPushingThread: Thread = null

  def onStart() {
    // Open a socket to the target address and keep reading from it
    logInfo("Connecting to " + host + ":" + port)
    val channel = SocketChannel.open()
    channel.configureBlocking(true)
    channel.connect(new InetSocketAddress(host, port))
    logInfo("Connected to " + host + ":" + port)

    val queue = new ArrayBlockingQueue[ByteBuffer](2)

    blockPushingThread = new Thread {
      setDaemon(true)
      override def run() {
        var nextBlockNumber = 0
        while (true) {
          val buffer = queue.take()
          nextBlockNumber += 1
          store(buffer)
        }
      }
    }
    blockPushingThread.start()

    val lengthBuffer = ByteBuffer.allocate(4)
    while (true) {
      lengthBuffer.clear()
      readFully(channel, lengthBuffer)
      lengthBuffer.flip()
      val length = lengthBuffer.getInt()
      val dataBuffer = ByteBuffer.allocate(length)
      readFully(channel, dataBuffer)
      dataBuffer.flip()
      logInfo("Read a block with " + length + " bytes")
      queue.put(dataBuffer)
    }
  }

  def onStop() {
    if (blockPushingThread != null) blockPushingThread.interrupt()
  }

  /** Read a buffer fully from a given Channel */
  private def readFully(channel: ReadableByteChannel, dest: ByteBuffer) {
    while (dest.position < dest.limit) {
      if (channel.read(dest) == -1) {
        throw new EOFException("End of channel")
      }
    }
  }
}
