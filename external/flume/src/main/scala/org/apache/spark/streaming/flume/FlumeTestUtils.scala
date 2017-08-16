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

import java.net.{InetSocketAddress, ServerSocket}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.{List => JList}
import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.avro.ipc.NettyTransceiver
import org.apache.avro.ipc.specific.SpecificRequestor
import org.apache.commons.lang3.RandomUtils
import org.apache.flume.source.avro
import org.apache.flume.source.avro.{AvroFlumeEvent, AvroSourceProtocol}
import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.channel.socket.SocketChannel
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.handler.codec.compression.{ZlibDecoder, ZlibEncoder}

import org.apache.spark.util.Utils
import org.apache.spark.SparkConf

/**
 * Share codes for Scala and Python unit tests
 */
private[flume] class FlumeTestUtils {

  private var transceiver: NettyTransceiver = null

  private val testPort: Int = findFreePort()

  def getTestPort(): Int = testPort

  /** Find a free port */
  private def findFreePort(): Int = {
    val candidatePort = RandomUtils.nextInt(1024, 65536)
    Utils.startServiceOnPort(candidatePort, (trialPort: Int) => {
      val socket = new ServerSocket(trialPort)
      socket.close()
      (null, trialPort)
    }, new SparkConf())._2
  }

  /** Send data to the flume receiver */
  def writeInput(input: JList[String], enableCompression: Boolean): Unit = {
    val testAddress = new InetSocketAddress("localhost", testPort)

    val inputEvents = input.asScala.map { item =>
      val event = new AvroFlumeEvent
      event.setBody(ByteBuffer.wrap(item.getBytes(StandardCharsets.UTF_8)))
      event.setHeaders(Collections.singletonMap("test", "header"))
      event
    }

    // if last attempted transceiver had succeeded, close it
    close()

    // Create transceiver
    transceiver = {
      if (enableCompression) {
        new NettyTransceiver(testAddress, new CompressionChannelFactory(6))
      } else {
        new NettyTransceiver(testAddress)
      }
    }

    // Create Avro client with the transceiver
    val client = SpecificRequestor.getClient(classOf[AvroSourceProtocol], transceiver)
    if (client == null) {
      throw new AssertionError("Cannot create client")
    }

    // Send data
    val status = client.appendBatch(inputEvents.asJava)
    if (status != avro.Status.OK) {
      throw new AssertionError("Sent events unsuccessfully")
    }
  }

  def close(): Unit = {
    if (transceiver != null) {
      transceiver.close()
      transceiver = null
    }
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
