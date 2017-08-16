/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.flume

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.channel.socket.SocketChannel
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.handler.codec.compression._
import org.scalatest.{BeforeAndAfter, Matchers}
import org.scalatest.concurrent.Eventually._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext, TestOutputStream}

class FlumeStreamSuite extends SparkFunSuite with BeforeAndAfter with Matchers with Logging {
  val conf = new SparkConf().setMaster("local[4]").setAppName("FlumeStreamSuite")
  var ssc: StreamingContext = null

  test("flume input stream") {
    testFlumeStream(testCompression = false)
  }

  test("flume input compressed stream") {
    testFlumeStream(testCompression = true)
  }

  /** Run test on flume stream */
  private def testFlumeStream(testCompression: Boolean): Unit = {
    val input = (1 to 100).map { _.toString }
    val utils = new FlumeTestUtils
    try {
      val outputQueue = startContext(utils.getTestPort(), testCompression)

      eventually(timeout(10 seconds), interval(100 milliseconds)) {
        utils.writeInput(input.asJava, testCompression)
      }

      eventually(timeout(10 seconds), interval(100 milliseconds)) {
        val outputEvents = outputQueue.asScala.toSeq.flatten.map { _.event }
        outputEvents.foreach {
          event =>
            event.getHeaders.get("test") should be("header")
        }
        val output = outputEvents.map(event => JavaUtils.bytesToString(event.getBody))
        output should be (input)
      }
    } finally {
      if (ssc != null) {
        ssc.stop()
      }
      utils.close()
    }
  }

  /** Setup and start the streaming context */
  private def startContext(
      testPort: Int, testCompression: Boolean): (ConcurrentLinkedQueue[Seq[SparkFlumeEvent]]) = {
    ssc = new StreamingContext(conf, Milliseconds(200))
    val flumeStream = FlumeUtils.createStream(
      ssc, "localhost", testPort, StorageLevel.MEMORY_AND_DISK, testCompression)
    val outputQueue = new ConcurrentLinkedQueue[Seq[SparkFlumeEvent]]
    val outputStream = new TestOutputStream(flumeStream, outputQueue)
    outputStream.register()
    ssc.start()
    outputQueue
  }

  /** Class to create socket channel with compression */
  private class CompressionChannelFactory(compressionLevel: Int)
    extends NioClientSocketChannelFactory {

    override def newChannel(pipeline: ChannelPipeline): SocketChannel = {
      val encoder = new ZlibEncoder(compressionLevel)
      pipeline.addFirst("deflater", encoder)
      pipeline.addFirst("inflater", new ZlibDecoder())
      super.newChannel(pipeline)
    }
  }
}
